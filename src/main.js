import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import * as cheerio from 'cheerio';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

const JUSTIA_BASE = 'https://www.justia.com';

// User-Agent pool for rotation
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
];

function randomUserAgent() {
    return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function randomDelay() {
    return Math.floor(Math.random() * 1000) + 500; // 500-1500ms
}

function toSlug(value) {
    return (value || '')
        .toString()
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '-')
        .replace(/[^a-z0-9-]/g, '')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '');
}

function absoluteUrl(maybeRelativeUrl, baseUrl) {
    if (!maybeRelativeUrl) return '';
    try {
        return new URL(maybeRelativeUrl, baseUrl || JUSTIA_BASE).toString();
    } catch {
        return '';
    }
}

function safeJsonParse(text) {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

// Extract lawyers from JSON-LD structured data
function extractLawyersFromJsonLd(data, pageUrl) {
    const results = [];

    // Handle array wrapper or @graph structure
    const items = Array.isArray(data) ? data : data['@graph'] || [data];

    for (const item of items) {
        if (!item || typeof item !== 'object') continue;

        // Check for ItemList schema (common for listing pages)
        if (item['@type'] === 'ItemList' && item.itemListElement) {
            const listItems = Array.isArray(item.itemListElement) ? item.itemListElement : [item.itemListElement];
            for (const listItem of listItems) {
                const nestedItem = listItem.item || listItem;
                if (nestedItem) {
                    const extracted = extractLawyersFromJsonLd(nestedItem, pageUrl);
                    results.push(...extracted);
                }
            }
            continue;
        }

        // Check if this is a lawyer/attorney/person entity
        const type = item['@type'] || '';
        const isLawyer =
            type.includes('Attorney') ||
            type.includes('Lawyer') ||
            type.includes('Person') ||
            type.includes('LegalService') ||
            type.includes('Organization');

        if (!isLawyer && !item.name) continue;

        const name = item.name || item.givenName || '';
        if (!name) continue; // Skip if no name

        const profileUrl = absoluteUrl(item.url || item['@id'] || '', pageUrl);

        // Extract practice areas
        const practiceAreas = (() => {
            if (item.knowsAbout) {
                return Array.isArray(item.knowsAbout)
                    ? item.knowsAbout.join(', ')
                    : item.knowsAbout;
            }
            if (item.areaServed || item.serviceArea) {
                const area = item.areaServed || item.serviceArea;
                return Array.isArray(area) ? area.join(', ') : area;
            }
            return '';
        })();

        // Extract location
        const location = (() => {
            if (typeof item.address === 'string') return item.address;
            if (item.address?.addressLocality || item.address?.addressRegion) {
                return [item.address.addressLocality, item.address.addressRegion]
                    .filter(Boolean)
                    .join(', ');
            }
            return item.location || '';
        })();

        results.push({
            name: name || 'Unknown Name',
            firmName: item.worksFor?.name || item.affiliation?.name || item.memberOf?.name || '',
            location,
            address: typeof item.address === 'string' ? item.address : (item.address?.streetAddress || location),
            phone: item.telephone || item.phone || '',
            email: item.email || '',
            website: profileUrl,
            practiceAreas,
            description: item.description || '',
            yearsLicensed: item.yearsInPractice || '',
            biography: null,
            education: null,
            barAdmissions: null,
            languages: null,
            scrapedAt: new Date().toISOString(),
        });
    }

    return results;
}


// Extract lawyers from HTML
function extractListingViaHtml({ html, pageUrl }) {
    const $ = cheerio.load(html);

    const allLawyerLinks = $('a[href^="/lawyers/"]').length;
    log.debug(`HTML extraction: found ${allLawyerLinks} total lawyer links`);

    const candidates = [];

    // Find all profile links
    $('a[href^="/lawyers/"]').each((_, el) => {
        const $a = $(el);
        const href = $a.attr('href') || '';

        // Filter for profile links (not category/search links)
        const pathParts = href.split('/').filter(p => p);
        if (pathParts.length < 2 || !href.includes('/lawyers/')) return;

        const profileUrl = absoluteUrl(href, pageUrl);
        const name = $a.text().trim();

        // Find the containing card/article
        const $card = $a.closest('article, li, div[class*="listing"], div[class*="card"], div[class*="result"]');
        if (!$card.length) return;

        // Extract data from card
        const firmName = $card.find('.firm-name, [class*="firm"], .law-firm, .organization').first().text().trim();
        const location = $card.find('.location, [class*="location"], .address, [itemprop="address"]').first().text().trim();

        const phone = (() => {
            const $tel = $card.find('a[href^="tel:"]').first();
            return $tel.text().trim() || $tel.attr('href')?.replace('tel:', '').trim() ||
                $card.find('.phone, [class*="phone"]').first().text().trim();
        })();

        const practiceAreas = (() => {
            const pieces = [];
            $card.find('a[href*="-law"], .practice-area, [class*="practice"]').each((_, n) => {
                const t = $(n).text().trim();
                if (t && t.length > 2 && t.length < 100) pieces.push(t);
            });
            return [...new Set(pieces)].slice(0, 10).join(', ');
        })();

        candidates.push({
            name: name || 'Unknown Name',
            firmName,
            location,
            address: location,
            phone,
            email: '',
            website: profileUrl,
            practiceAreas,
            description: '',
            yearsLicensed: '',
            biography: null,
            education: null,
            barAdmissions: null,
            languages: null,
            scrapedAt: new Date().toISOString(),
        });
    });

    // De-dupe by website
    const seen = new Set();
    const lawyers = candidates.filter((r) => {
        const key = r.website;
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
    });

    log.debug(`HTML extraction: ${candidates.length} candidates, ${lawyers.length} unique lawyers`);
    return lawyers;
}

// Find next page URL
function findNextPageUrl({ html, pageUrl }) {
    const $ = cheerio.load(html);

    // Try rel="next"
    const relNext = $('a[rel="next"]').attr('href');
    if (relNext) return absoluteUrl(relNext, pageUrl);

    // Try common pagination text
    const nextText = $('a').filter((_, el) => {
        const t = $(el).text().trim().toLowerCase();
        return t === 'next' || t === 'next ›' || t === '›' || t === '>' || t.includes('next page');
    }).first().attr('href');
    if (nextText) return absoluteUrl(nextText, pageUrl);

    // Try aria-label
    const ariaNext = $('a[aria-label*="Next"], a[aria-label*="next"]').first().attr('href');
    if (ariaNext) return absoluteUrl(ariaNext, pageUrl);

    // Try pagination number increment
    const $pagination = $('.pagination, .pager, nav[role="navigation"]');
    if ($pagination.length) {
        const $current = $pagination.find('a[aria-current="page"], .current, .active, .selected').first();
        if ($current.length) {
            const currentText = $current.text().trim();
            const currentNum = Number.parseInt(currentText, 10);
            if (Number.isFinite(currentNum)) {
                const $nextNum = $pagination.find('a').filter((_, el) => {
                    const t = $(el).text().trim();
                    return Number.parseInt(t, 10) === currentNum + 1;
                }).first();
                const href = $nextNum.attr('href');
                if (href) return absoluteUrl(href, pageUrl);
            }
        }
    }

    return '';
}

// Enrich profile with detail page data
async function enrichProfileWithDetails({ page, profileUrl, baseData }) {
    try {
        await page.goto(profileUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForLoadState('domcontentloaded').catch(() => { });

        const html = await page.content();
        const $ = cheerio.load(html);

        const email = $('a[href^="mailto:"]').first().attr('href')?.replace('mailto:', '').trim() || null;

        const biography = $('.biography, #biography, [class*="bio"], .profile-description, .about').first().text().trim() || null;

        const education = [];
        $('.education li, [class*="education"] li, .education-list li').each((_, el) => {
            const t = $(el).text().trim();
            if (t) education.push(t);
        });

        const barAdmissions = [];
        $('.admissions li, [class*="admission"] li, .bar-admissions li').each((_, el) => {
            const t = $(el).text().trim();
            if (t) barAdmissions.push(t);
        });

        const languages = [];
        $('.languages li, [class*="language"] li').each((_, el) => {
            const t = $(el).text().trim();
            if (t) languages.push(t);
        });

        return {
            ...baseData,
            email: email || baseData.email,
            biography: biography || baseData.biography,
            education: education.length ? education : baseData.education,
            barAdmissions: barAdmissions.length ? barAdmissions : baseData.barAdmissions,
            languages: languages.length ? languages : baseData.languages,
        };
    } catch (err) {
        log.warning(`Failed to enrich profile ${profileUrl}:`, err.message);
        return baseData;
    }
}

// Main execution
try {
    const input = (await Actor.getInput()) || {};

    const debug = input.debug ?? false;
    const maxLawyers = Number.isFinite(input.maxLawyers) ? input.maxLawyers : 50;
    const maxPages = Number.isFinite(input.maxPages) ? input.maxPages : 5;
    const fetchFullProfiles = input.fetchFullProfiles ?? false;

    // Apify-recommended proxy configuration with checkAccess
    const proxyConfiguration = await Actor.createProxyConfiguration({
        ...(input.proxyConfiguration || { useApifyProxy: true }),
        checkAccess: true,
    });

    const searchUrl = (() => {
        if (input.searchUrl && input.searchUrl.trim()) return input.searchUrl.trim();
        const practiceArea = toSlug(input.practiceArea);
        const location = toSlug(input.location);
        if (!practiceArea || !location) {
            throw new Error('Invalid input: provide searchUrl OR both practiceArea and location');
        }
        return `${JUSTIA_BASE}/lawyers/${practiceArea}/${location}`;
    })();

    log.info('Starting Justia Lawyer Scraper (Camoufox Stealth Mode)', {
        searchUrl,
        maxLawyers,
        maxPages,
        fetchFullProfiles,
        debug,
    });

    const seenWebsites = new Set();
    let totalScraped = 0;
    let pagesProcessed = 0;

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: maxPages > 0 ? maxPages : undefined,
        maxConcurrency: 3,
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 180,
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: await proxyConfiguration.newUrl(),
                geoip: true,
                os: 'windows',
                screen: {
                    minWidth: 1280,
                    maxWidth: 1920,
                    minHeight: 720,
                    maxHeight: 1080,
                },
            }),
        },

        async requestHandler({ page, request, crawler: selfCrawler }) {
            log.info(`Processing page ${pagesProcessed + 1}`, { url: request.url });

            // Navigate with stealth optimizations
            await page.setExtraHTTPHeaders({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': randomUserAgent(),
            });

            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
            await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => { });

            // Random human-like delay
            await page.waitForTimeout(randomDelay());

            const html = await page.content();

            let lawyers = [];
            let extractionMethod = 'none';

            // Priority 1: JSON-LD Structured Data
            const jsonLdScripts = await page.$$('script[type="application/ld+json"]');
            for (const script of jsonLdScripts) {
                const jsonText = await script.textContent();
                const data = safeJsonParse(jsonText);
                if (data) {
                    const extracted = extractLawyersFromJsonLd(data, request.url);
                    if (extracted.length > 0) {
                        lawyers = extracted;
                        extractionMethod = 'JSON-LD';
                        log.info(`Extracted ${lawyers.length} lawyers via JSON-LD`);
                        break;
                    }
                }
            }

            // Priority 2: HTML Parsing (fallback)
            if (!lawyers.length) {
                lawyers = extractListingViaHtml({ html, pageUrl: request.url });
                if (lawyers.length) {
                    extractionMethod = 'HTML Parsing';
                    log.info(`Extracted ${lawyers.length} lawyers via HTML parsing`);
                }
            }

            // Debug logging before filtering
            if (debug && lawyers.length > 0) {
                log.debug('Raw extracted lawyers:', {
                    count: lawyers.length,
                    sample: lawyers[0],
                    hasWebsites: lawyers.filter(l => l.website).length,
                });
            }

            // Filter duplicates and apply limits
            const unique = [];
            let filtered = 0;
            for (const lawyer of lawyers) {
                // Generate dedup key: use website if available, otherwise use name+location
                const dedupKey = lawyer.website || `${lawyer.name}|${lawyer.location}|${lawyer.firmName}`;

                if (seenWebsites.has(dedupKey)) {
                    filtered++;
                    continue;
                }
                if (maxLawyers > 0 && totalScraped >= maxLawyers) break;

                seenWebsites.add(dedupKey);
                unique.push(lawyer);
                totalScraped++;
            }

            if (debug && filtered > 0) {
                log.debug(`Filtered out ${filtered} duplicate lawyers`);
            }

            // Enrich with full profiles if requested
            if (fetchFullProfiles && unique.length) {
                const enriched = [];
                for (const baseLawyer of unique) {
                    if (!baseLawyer.website) {
                        enriched.push(baseLawyer);
                        continue;
                    }
                    const detailed = await enrichProfileWithDetails({
                        page,
                        profileUrl: baseLawyer.website,
                        baseData: baseLawyer,
                    });
                    enriched.push(detailed);
                }
                await Actor.pushData(enriched);
            } else if (unique.length) {
                await Actor.pushData(unique);
            }

            pagesProcessed++;

            log.info('Saved lawyers', {
                page: pagesProcessed,
                savedThisPage: unique.length,
                totalScraped,
                extractionMethod,
                extractedBeforeFilter: lawyers.length,
            });

            // Save debug snapshot if no lawyers found
            if (debug && lawyers.length === 0) {
                await Actor.setValue(
                    `DEBUG_EMPTY_PAGE_${pagesProcessed}`,
                    {
                        url: request.url,
                        title: await page.title(),
                        htmlSnippet: html.slice(0, 5000),
                        timestamp: new Date().toISOString(),
                    },
                    { contentType: 'application/json' }
                );
            }

            // Check if we should continue
            if (maxLawyers > 0 && totalScraped >= maxLawyers) {
                log.info('Reached maxLawyers limit, stopping');
                return;
            }

            if (maxPages > 0 && pagesProcessed >= maxPages) {
                log.info('Reached maxPages limit, stopping');
                return;
            }

            // Find and enqueue next page
            const nextUrl = findNextPageUrl({ html, pageUrl: request.url });
            if (nextUrl && nextUrl !== request.url) {
                log.info('Enqueueing next page', { nextUrl });
                await selfCrawler.addRequests([{ url: nextUrl, uniqueKey: nextUrl }]);
            } else {
                log.info('No next page found, crawler will finish');
            }
        },

        failedRequestHandler({ request }, error) {
            log.error(`Request ${request.url} failed:`, error.message);
        },
    });

    await crawler.run([searchUrl]);

    const stats = {
        totalLawyersScraped: totalScraped,
        pagesProcessed,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('SCRAPER_STATISTICS', stats);
    log.info('Scraper finished successfully', stats);

} catch (err) {
    log.exception(err, 'Actor failed');
    throw err;
} finally {
    await Actor.exit();
}
