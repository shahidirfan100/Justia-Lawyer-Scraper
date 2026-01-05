import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';

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

function buildHeaders() {
    return {
        Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': randomUserAgent(),
    };
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

function isBlockedHtml(html) {
    if (!html) return false;
    const lower = html.toLowerCase();
    return (
        lower.includes('just a moment') ||
        lower.includes('verify you are human') ||
        lower.includes('cf-browser-verification') ||
        lower.includes('access denied') ||
        lower.includes('security of your connection')
    );
}

// Extract lawyers from arbitrary API-like JSON payloads (Next.js data, internal APIs)
function extractLawyersFromApiPayload(payload, pageUrl) {
    const results = [];
    const seen = new Set();

    function pushCandidate(node) {
        const name = node.name || node.fullName || node.title || '';
        if (!name || name.length < 2) return;

        const profileUrl = absoluteUrl(
            node.url || node.profileUrl || node.link || node.website || node.permalink,
            pageUrl,
        );

        const location =
            node.location ||
            node.city ||
            node.address?.city ||
            node.address?.addressLocality ||
            node.address?.region ||
            node.state ||
            '';

        const practiceAreas = (() => {
            const areas = node.practiceAreas || node.specialties || node.categories || node.tags;
            if (!areas) return '';
            if (Array.isArray(areas)) return areas.map(String).join(', ');
            if (typeof areas === 'string') return areas;
            return '';
        })();

        const firmName =
            node.firmName ||
            node.company ||
            node.organization ||
            node.orgName ||
            node.office ||
            node.lawFirm ||
            '';

        const phone =
            node.phone ||
            node.telephone ||
            node.tel ||
            node.contactPhone ||
            node.mobile ||
            (typeof node.contact === 'object' ? node.contact?.phone : '') ||
            '';

        const email =
            node.email ||
            node.mail ||
            node.contactEmail ||
            (typeof node.contact === 'object' ? node.contact?.email : '') ||
            '';

        const dedupKey = `${name}|${profileUrl || node.id || node.slug || location}`;
        if (seen.has(dedupKey)) return;
        seen.add(dedupKey);

        results.push({
            name,
            firmName,
            location,
            address:
                typeof node.address === 'string'
                    ? node.address
                    : node.address?.streetAddress || location || '',
            phone: phone || '',
            email: email || '',
            website: profileUrl,
            practiceAreas,
            description: node.description || node.summary || '',
            yearsLicensed: node.yearsLicensed || node.experience || '',
            biography: node.biography || node.bio || null,
            education: node.education || node.schools || null,
            barAdmissions: node.barAdmissions || node.admissions || null,
            languages: node.languages || null,
            scrapedAt: new Date().toISOString(),
        });
    }

    function walk(node) {
        if (!node) return;
        if (Array.isArray(node)) {
            for (const item of node) walk(item);
            return;
        }
        if (typeof node !== 'object') return;

        const keys = Object.keys(node).map((k) => k.toLowerCase());
        const hasName = Boolean(node.name || node.fullName || node.title);
        const looksLikeLawyer =
            hasName ||
            keys.some((k) => k.includes('attorney') || k.includes('lawyer') || k.includes('profile'));

        if (looksLikeLawyer) pushCandidate(node);

        for (const val of Object.values(node)) walk(val);
    }

    walk(payload);
    return results;
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

        const languages = (() => {
            if (Array.isArray(item.knowsLanguage)) return item.knowsLanguage;
            if (typeof item.knowsLanguage === 'string') return item.knowsLanguage.split(',').map((s) => s.trim());
            return null;
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
            languages,
            scrapedAt: new Date().toISOString(),
        });
    }

    return results;
}

function extractLawyersFromJsonLdHtml({ html, pageUrl }) {
    const $ = cheerio.load(html);
    const found = [];
    $('script[type="application/ld+json"]').each((_, el) => {
        const text = $(el).contents().text();
        const data = safeJsonParse(text);
        if (data) found.push(...extractLawyersFromJsonLd(data, pageUrl));
    });
    return found;
}

function extractLawyersFromNextData({ html, pageUrl }) {
    const $ = cheerio.load(html);
    const scriptText = $('#__NEXT_DATA__').first().text() || $('script#__NEXT_DATA__').first().text();
    if (!scriptText) return [];
    const data = safeJsonParse(scriptText);
    if (!data) return [];
    return extractLawyersFromApiPayload(data, pageUrl);
}


// Extract lawyers from HTML
function extractListingViaHtml({ html, pageUrl }) {
    const $ = cheerio.load(html);

    const allLawyerLinks = $('a[href^="/lawyers/"]').length;
    log.debug(`HTML extraction: found ${allLawyerLinks} total lawyer links`);

    const candidates = [];

    $('a[href^="/lawyers/"]').each((_, el) => {
        const $a = $(el);
        const href = $a.attr('href') || '';
        const pathParts = href.split('/').filter((p) => p);
        if (pathParts.length < 2 || !href.includes('/lawyers/')) return;

        const profileUrl = absoluteUrl(href, pageUrl);
        const $card = $a.closest(
            'article, li, .lawyer, .lawyer-card, .profile-card, div[class*="listing"], div[class*="card"], div[class*="result"]',
        );
        if (!$card.length) return;

        const name =
            $card.find('[itemprop="name"], .lawyer-name, .name, h2, h3').first().text().trim() ||
            $a.text().trim() ||
            'Unknown Name';

        const firmName =
            $card
                .find(
                    '[itemprop="worksFor"], .firm-name, .law-firm, .organization, .company, .lawyer-company, [class*="firm"]',
                )
                .first()
                .text()
                .trim() || '';

        const street = $card.find('[itemprop="streetAddress"]').first().text().trim();
        const locality = $card.find('[itemprop="addressLocality"], .city, .locality').first().text().trim();
        const region = $card.find('[itemprop="addressRegion"], .state, .region').first().text().trim();
        const postal = $card.find('[itemprop="postalCode"]').first().text().trim();
        const addressParts = [street, locality, region, postal].filter(Boolean);
        const address = addressParts.join(', ');
        const location =
            $card.find('.location, [class*="location"], .address, [itemprop="address"]').first().text().trim() ||
            [locality, region].filter(Boolean).join(', ') ||
            address;

        const phone = (() => {
            const $tel = $card.find('a[href^="tel:"]').first();
            const telText = $tel.text().trim() || $tel.attr('href')?.replace('tel:', '').trim();
            return telText || $card.find('.phone, [class*="phone"], [itemprop="telephone"]').first().text().trim();
        })();

        const email =
            $card.find('a[href^="mailto:"]').first().attr('href')?.replace('mailto:', '').trim() ||
            $card.find('[itemprop="email"]').first().text().trim() ||
            '';

        const practiceAreas = (() => {
            const pieces = [];
            $card.find('.practice-areas a, .practice-areas li, .practice-area, [class*="practice"]').each((_, n) => {
                const t = $(n).text().trim();
                if (t && t.length > 2 && t.length < 100) pieces.push(t);
            });
            return [...new Set(pieces)].slice(0, 10).join(', ');
        })();

        const description =
            $card
                .find('.description, .summary, .lawyer-description, .profile-description, .about')
                .first()
                .text()
                .trim() || '';

        const yearsLicensed = (() => {
            const text = $card.text();
            const match = text.match(/Licensed\\s+for\\s+(\\d+)\\s+years?/i);
            return match ? match[1] : '';
        })();

        const languages = (() => {
            const langs = [];
            $card.find('.languages li, [class*="language"]').each((_, n) => {
                const t = $(n).text().trim();
                if (t) langs.push(t);
            });
            return langs.length ? langs : null;
        })();

        candidates.push({
            name,
            firmName,
            location,
            address: address || location,
            phone,
            email,
            website: profileUrl,
            practiceAreas,
            description,
            yearsLicensed,
            biography: null,
            education: null,
            barAdmissions: null,
            languages,
            scrapedAt: new Date().toISOString(),
        });
    });

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

async function fetchHtmlWithGot({ url, proxyConfiguration, headers }) {
    const proxyUrl = await proxyConfiguration.newUrl().catch(() => null);
    try {
        const response = await gotScraping({
            url,
            proxyUrl,
            headers,
            timeout: { request: 45000 },
        });
        return response.body?.toString() || '';
    } catch (err) {
        log.warning(`Profile fetch failed (proxy) for ${url}: ${err.message}`);
        if (!proxyUrl) return '';
    }

    try {
        const response = await gotScraping({
            url,
            headers,
            timeout: { request: 45000 },
        });
        return response.body?.toString() || '';
    } catch (err) {
        log.warning(`Profile fetch failed without proxy for ${url}: ${err.message}`);
        return '';
    }
}

// Enrich profile with detail page data (HTTP-only)
async function enrichProfileWithDetailsHttp({ profileUrl, baseData, proxyConfiguration, headers }) {
    try {
        const html = await fetchHtmlWithGot({ url: profileUrl, proxyConfiguration, headers });
        if (!html || isBlockedHtml(html)) {
            if (html && isBlockedHtml(html)) {
                log.warning(`Blocked on profile page, skipping enrichment: ${profileUrl}`);
            }
            return baseData;
        }

        const $ = cheerio.load(html);

        const email =
            $('a[href^="mailto:"]').first().attr('href')?.replace('mailto:', '').trim() ||
            $('[itemprop="email"]').first().text().trim() ||
            null;

        const biography =
            $('.biography, #biography, [class*="bio"], .profile-description, .about').first().text().trim() || null;

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

        const practiceAreas = (() => {
            const pieces = [];
            $('.practice-areas a, .practice-areas li, .practice-area, [class*="practice"]').each((_, el) => {
                const t = $(el).text().trim();
                if (t && t.length > 2 && t.length < 100) pieces.push(t);
            });
            return [...new Set(pieces)].slice(0, 10).join(', ');
        })();

        const firmName =
            $('[itemprop="worksFor"], .firm-name, .law-firm, .organization, .company, .lawyer-company, [class*="firm"]')
                .first()
                .text()
                .trim() || baseData.firmName;

        const phone =
            $('a[href^="tel:"]').first().text().trim() ||
            $('a[href^="tel:"]').first().attr('href')?.replace('tel:', '').trim() ||
            $('[itemprop="telephone"]').first().text().trim() ||
            baseData.phone;

        const street = $('[itemprop="streetAddress"]').first().text().trim();
        const locality = $('[itemprop="addressLocality"], .city, .locality').first().text().trim();
        const region = $('[itemprop="addressRegion"], .state, .region').first().text().trim();
        const postal = $('[itemprop="postalCode"]').first().text().trim();
        const addressParts = [street, locality, region, postal].filter(Boolean);
        const address = addressParts.join(', ') || baseData.address;
        const location =
            $('[class*="location"], .address, [itemprop="address"]').first().text().trim() ||
            [locality, region].filter(Boolean).join(', ') ||
            baseData.location;

        const yearsLicensed = (() => {
            const text = $('body').text();
            const match = text.match(/Licensed\\s+for\\s+(\\d+)\\s+years?/i);
            return match ? match[1] : baseData.yearsLicensed;
        })();

        const ldLawyers = extractLawyersFromJsonLdHtml({ html, pageUrl: profileUrl });
        const ld = ldLawyers[0];

        const mergedPractice = practiceAreas || ld?.practiceAreas || baseData.practiceAreas;
        const mergedAddress = address || ld?.address || baseData.address;
        const mergedLocation = location || ld?.location || baseData.location;
        const mergedPhone = phone || ld?.phone || baseData.phone;
        const mergedEmail = email || ld?.email || baseData.email;
        const mergedFirm = firmName || ld?.firmName || baseData.firmName;
        const mergedDesc = biography || ld?.description || baseData.description;

        return {
            ...baseData,
            email: mergedEmail || baseData.email,
            phone: mergedPhone,
            firmName: mergedFirm,
            address: mergedAddress,
            location: mergedLocation,
            practiceAreas: mergedPractice,
            description: mergedDesc || baseData.description,
            yearsLicensed,
            biography: biography || ld?.biography || baseData.biography,
            education: education.length ? education : ld?.education || baseData.education,
            barAdmissions: barAdmissions.length ? barAdmissions : ld?.barAdmissions || baseData.barAdmissions,
            languages: languages.length ? languages : ld?.languages || baseData.languages,
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
    const fetchFullProfiles = input.fetchFullProfiles ?? true;

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

    log.info('Starting Justia Lawyer Scraper (HTTP mode: got-scraping)', {
        searchUrl,
        maxLawyers,
        maxPages,
        fetchFullProfiles,
        debug,
    });

    const seenWebsites = new Set();
    let totalScraped = 0;
    let pagesProcessed = 0;

    const crawler = new CheerioCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: maxPages > 0 ? maxPages : undefined,
        maxConcurrency: 8,
        requestHandlerTimeoutSecs: 180,
        maxRequestRetries: 4,
        navigationTimeoutSecs: 45,
        useSessionPool: true,
        persistCookiesPerSession: true,
        sessionPoolOptions: {
            blockedStatusCodes: [],
            sessionOptions: {
                maxUsageCount: 20,
            },
        },
        // Stealth configuration via preNavigationHooks (Apify recommended)
        preNavigationHooks: [
            async (crawlingContext, gotOptions) => {
                const { session, request } = crawlingContext;
                // Full stealth config for got-scraping
                gotOptions.headerGeneratorOptions = {
                    devices: ['desktop'],
                    operatingSystems: ['windows'],
                    browsers: [{ name: 'chrome', minVersion: 120 }],
                    locales: ['en-US'],
                };
                gotOptions.useHeaderGenerator = true;
                gotOptions.sessionToken = session;
                gotOptions.retry = {
                    limit: 2,
                    statusCodes: [408, 429, 500, 502, 503, 504],
                };
                // Set referer header
                try {
                    const urlObj = new URL(request.url);
                    gotOptions.headers = {
                        ...(gotOptions.headers || {}),
                        referer: urlObj.origin + '/',
                    };
                } catch {
                    // Ignore URL parsing errors
                }
            },
        ],

        async requestHandler({ request, $, response, body, crawler: selfCrawler, session, proxyInfo }) {
            log.info(`Processing page ${pagesProcessed + 1}`, { url: request.url });

            // Log proxy and session info on success
            log.debug('Request successful', {
                url: request.url,
                proxyUrl: proxyInfo?.url,
                sessionId: session?.id,
                statusCode: response?.statusCode,
            });

            const htmlSnapshot =
                typeof body === 'string'
                    ? body
                    : body
                        ? body.toString()
                        : $.html() || '';
            const statusCode = response?.statusCode;
            const blocked = statusCode === 403 || isBlockedHtml(htmlSnapshot);

            if (blocked) {
                log.warning('Blocked or challenge page detected on listing', {
                    url: request.url,
                    statusCode,
                    retryCount: request.retryCount,
                });
                if (session) session.retire();
                if (request.retryCount < 2) {
                    throw new Error(`Blocked status ${statusCode || 'unknown'}`);
                }
            }

            let lawyers = [];
            const extractionSources = new Set();

            if (htmlSnapshot) {
                const jsonLdLawyers = extractLawyersFromJsonLdHtml({ html: htmlSnapshot, pageUrl: request.url });
                if (jsonLdLawyers.length) {
                    lawyers.push(...jsonLdLawyers);
                    extractionSources.add('jsonld');
                }

                const nextDataLawyers = extractLawyersFromNextData({ html: htmlSnapshot, pageUrl: request.url });
                if (nextDataLawyers.length) {
                    lawyers.push(...nextDataLawyers);
                    extractionSources.add('next-data');
                }

                const htmlLawyers = extractListingViaHtml({ html: htmlSnapshot, pageUrl: request.url });
                if (htmlLawyers.length) {
                    lawyers.push(...htmlLawyers);
                    extractionSources.add('html');
                }
            }

            // Debug logging before filtering
            if (debug && lawyers.length > 0) {
                log.debug('Raw extracted lawyers:', {
                    count: lawyers.length,
                    sample: lawyers[0],
                    sources: Array.from(extractionSources),
                    hasWebsites: lawyers.filter((l) => l.website).length,
                });
            }

            // Filter duplicates and apply limits
            const unique = [];
            let filtered = 0;
            for (const lawyer of lawyers) {
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

            if (filtered > 0) {
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
                    await new Promise((resolve) => setTimeout(resolve, randomDelay()));
                    const detailed = await enrichProfileWithDetailsHttp({
                        profileUrl: baseLawyer.website,
                        baseData: baseLawyer,
                        proxyConfiguration,
                        headers: buildHeaders(),
                    });
                    enriched.push(detailed);
                }
                await Actor.pushData(enriched);
            } else if (unique.length) {
                if (!fetchFullProfiles) {
                    log.warningOnce(
                        'fetchFullProfiles is disabled, so fields like biography, education, bar admissions, and languages may be empty.'
                    );
                }
                await Actor.pushData(unique);
            }

            pagesProcessed++;

            log.info('Saved lawyers', {
                page: pagesProcessed,
                savedThisPage: unique.length,
                totalScraped,
                extractionSources: Array.from(extractionSources),
                extractedBeforeFilter: lawyers.length,
            });

            if (debug || lawyers.length === 0) {
                await Actor.setValue(
                    `DEBUG_PAGE_${pagesProcessed}`,
                    JSON.stringify({
                        url: request.url,
                        title: $('title').first().text().trim() || null,
                        htmlSnippet: htmlSnapshot.slice(0, 8000),
                        isBlocked: blocked,
                        timestamp: new Date().toISOString(),
                    }),
                    { contentType: 'application/json' }
                );
            }

            if (maxLawyers > 0 && totalScraped >= maxLawyers) {
                log.info('Reached maxLawyers limit, stopping');
                return;
            }

            if (maxPages > 0 && pagesProcessed >= maxPages) {
                log.info('Reached maxPages limit, stopping');
                return;
            }

            const nextUrl = findNextPageUrl({ html: htmlSnapshot, pageUrl: request.url });
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
