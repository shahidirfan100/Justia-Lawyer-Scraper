import { Actor, log } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { gotScraping } from 'got-scraping';
import * as cheerio from 'cheerio';
import { launchOptions as camoufoxLaunchOptions } from 'camoufox-js';
import { firefox } from 'playwright';

await Actor.init();

const JUSTIA_BASE = 'https://www.justia.com';

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

function isBlockedHtml(html) {
    if (!html) return false;
    const sample = html.slice(0, 5000).toLowerCase();
    return (
        sample.includes('just a moment') ||
        sample.includes('checking your browser') ||
        sample.includes('cf-browser-verification') ||
        sample.includes('cloudflare')
    );
}

function safeJsonParse(text) {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

async function fetchHttp(url, { proxyUrl, timeoutMillis = 20000 } = {}) {
    const response = await gotScraping({
        url,
        proxyUrl,
        timeout: { request: timeoutMillis },
        headers: {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
        },
        retry: { limit: 1 },
        followRedirect: true,
    });

    return {
        url: response.url || url,
        statusCode: response.statusCode,
        body: response.body || '',
        headers: response.headers || {},
    };
}

function extractLawyersFromJsonObject(root, pageUrl) {
    const results = [];

    const stack = [root];
    const visited = new Set();

    while (stack.length) {
        const node = stack.pop();
        if (!node || typeof node !== 'object') continue;
        if (visited.has(node)) continue;
        visited.add(node);

        // Common patterns
        const arraysToCheck = [
            node.lawyers,
            node.attorneys,
            node.results,
            node.items,
            node.data?.lawyers,
            node.data?.attorneys,
            node.data?.results,
        ].filter(Boolean);

        for (const arr of arraysToCheck) {
            if (!Array.isArray(arr)) continue;
            for (const item of arr) {
                const name = item?.name || item?.fullName || item?.title || '';
                const profileUrl = item?.url || item?.profileUrl || item?.link || '';
                const absoluteProfileUrl = absoluteUrl(profileUrl, pageUrl);

                if (!name && !absoluteProfileUrl) continue;

                results.push({
                    name: name || 'Unknown Name',
                    firmName: item?.firmName || item?.firm || item?.organization?.name || '',
                    location: item?.location || item?.city || item?.region || '',
                    address: item?.address || '',
                    phone: item?.phone || item?.telephone || '',
                    email: item?.email || '',
                    website: absoluteProfileUrl,
                    practiceAreas: Array.isArray(item?.practiceAreas)
                        ? item.practiceAreas.join(', ')
                        : (item?.practiceAreas || item?.specialties || ''),
                    description: item?.description || item?.bio || '',
                    yearsLicensed: item?.yearsLicensed || item?.licensedSince || '',
                    biography: null,
                    education: null,
                    barAdmissions: null,
                    languages: null,
                    scrapedAt: new Date().toISOString(),
                });
            }
        }

        // Traverse children
        for (const value of Object.values(node)) {
            if (!value) continue;
            if (typeof value === 'object') stack.push(value);
        }
    }

    // De-dup by website
    const seen = new Set();
    return results.filter((r) => {
        const key = r.website || `${r.name}|${r.location}|${r.firmName}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

function tryExtractApiUrlFromHtml(html, pageUrl) {
    if (!html) return '';

    const patterns = [
        /https?:\/\/www\.justia\.com\/[^"'\s>]+\.json/gi,
        /https?:\/\/www\.justia\.com\/api\/[^"'\s>]+/gi,
        /\/[a-z0-9/_-]+\.json\b/gi,
        /\/[a-z0-9/_-]*api[a-z0-9/_-]*\b/gi,
    ];

    for (const re of patterns) {
        const matches = html.match(re) || [];
        for (const m of matches) {
            const abs = absoluteUrl(m, pageUrl);
            if (abs && abs.startsWith(JUSTIA_BASE)) return abs;
        }
    }

    // Also check common link rel alternate
    const $ = cheerio.load(html);
    const alt = $('link[rel="alternate"][type="application/json"]').attr('href');
    if (alt) {
        const abs = absoluteUrl(alt, pageUrl);
        if (abs) return abs;
    }

    return '';
}

async function extractListingViaJsonApi({ pageUrl, html, proxyUrl }) {
    // Priority: call an internal JSON endpoint if discoverable.
    const apiUrl = tryExtractApiUrlFromHtml(html, pageUrl);
    if (!apiUrl) return { lawyers: [], apiUrlTried: '' };

    log.info(`API-first: discovered candidate JSON endpoint: ${apiUrl}`);

    const r = await fetchHttp(apiUrl, { proxyUrl, timeoutMillis: 20000 });
    const data = safeJsonParse(r.body);
    if (!data) {
        log.debug('API-first: candidate endpoint did not return JSON');
        return { lawyers: [], apiUrlTried: apiUrl };
    }

    const lawyers = extractLawyersFromJsonObject(data, pageUrl);
    return { lawyers, apiUrlTried: apiUrl };
}

function extractListingViaEmbeddedJson({ html, pageUrl }) {
    // Fallback: parse embedded JSON blobs from scripts.
    const $ = cheerio.load(html);
    const scripts = $('script:not([src])');
    log.debug(`Embedded JSON: checking ${scripts.length} script tags`);

    for (const el of scripts.toArray()) {
        const text = ($(el).text() || '').trim();
        if (!text) continue;

        // Quickly skip scripts that don't look relevant
        const lower = text.toLowerCase();
        if (!lower.includes('lawyer') && !lower.includes('attorney') && !lower.includes('results')) continue;

        // Try whole script as JSON
        const direct = safeJsonParse(text);
        if (direct) {
            const lawyers = extractLawyersFromJsonObject(direct, pageUrl);
            if (lawyers.length > 0) return lawyers;
        }

        // Try extracting a JSON object substring
        const match = text.match(/\{[\s\S]*\}/);
        if (match) {
            const parsed = safeJsonParse(match[0]);
            if (parsed) {
                const lawyers = extractLawyersFromJsonObject(parsed, pageUrl);
                if (lawyers.length > 0) return lawyers;
            }
        }
    }

    return [];
}

function findBestCardContainer($, $a) {
    const containers = ['article', 'li', 'div'];
    for (const tag of containers) {
        const $c = $a.closest(tag);
        if ($c.length) return $c;
    }
    return $a.parent();
}

function isLikelyProfileHref(href) {
    if (!href) return false;
    // Accept any /lawyers/ link that has at least 3 path segments
    // Examples: /lawyers/john-doe-123456 or /lawyers/state/city/name
    const parts = href.split('/').filter(p => p);
    return /\/lawyers\//.test(href) && parts.length >= 2;
}

function extractListingViaHtml({ html, pageUrl }) {
    const $ = cheerio.load(html);

    const allLawyerLinks = $('a[href^="/lawyers/"]').length;
    log.debug(`HTML extraction: found ${allLawyerLinks} total lawyer links`);

    const candidates = [];
    $('a[href^="/lawyers/"]').each((_, el) => {
        const $a = $(el);
        const href = $a.attr('href') || '';
        if (!isLikelyProfileHref(href)) return;

        const profileUrl = absoluteUrl(href, pageUrl);
        const name = ($a.text() || '').trim();
        const $card = findBestCardContainer($, $a);

        const firmName = ($card.find('[class*="firm"], .firm, .firm-name, .law-firm').first().text() || '').trim();
        const location = ($card.find('[class*="location"], .location, .address, [itemprop="address"]').first().text() || '').trim();

        const phone = (
            ($card.find('a[href^="tel:"]').first().text() || '').trim() ||
            (($card.find('a[href^="tel:"]').first().attr('href') || '').replace('tel:', '').trim()) ||
            ($card.find('[class*="phone"], .phone').first().text() || '').trim()
        );

        const practiceAreas = (() => {
            const pieces = [];
            $card.find('a[href*="/lawyers/"][href*="-law/"], a[href*="/lawyers/"][href*="-lawyers"], [class*="practice"], .practice-areas li, .practice-areas a').each((_, n) => {
                const t = ($(n).text() || '').trim();
                if (t && t.length > 2) pieces.push(t);
            });
            const joined = [...new Set(pieces)].slice(0, 20).join(', ');
            return joined;
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

    // De-dupe
    const seen = new Set();
    const lawyers = candidates.filter((r) => {
        const key = r.website;
        if (!key) return false;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });

    log.debug(`HTML extraction: ${candidates.length} candidates, ${lawyers.length} unique lawyers`);

    return lawyers;
}

function findNextPageUrl({ html, pageUrl }) {
    const $ = cheerio.load(html);

    const relNext = $('a[rel="next"]').attr('href');
    if (relNext) return absoluteUrl(relNext, pageUrl);

    const nextText = $('a').filter((_, el) => {
        const t = ($(el).text() || '').trim().toLowerCase();
        return t === 'next' || t === 'next ›' || t === '›' || t === '>';
    }).first().attr('href');
    if (nextText) return absoluteUrl(nextText, pageUrl);

    const ariaNext = $('a[aria-label*="Next"], a[aria-label*="next"]').first().attr('href');
    if (ariaNext) return absoluteUrl(ariaNext, pageUrl);

    const $pagination = $('.pagination');
    if ($pagination.length) {
        const $current = $pagination.find('a[aria-current="page"], .current, .active').first();
        if ($current.length) {
            const currentText = ($current.text() || '').trim();
            const currentNum = Number.parseInt(currentText, 10);
            if (Number.isFinite(currentNum)) {
                const $nextNum = $pagination.find('a').filter((_, el) => {
                    const t = ($(el).text() || '').trim();
                    return Number.parseInt(t, 10) === currentNum + 1;
                }).first();
                const href = $nextNum.attr('href');
                if (href) return absoluteUrl(href, pageUrl);
            }
        }
    }

    return '';
}

async function fetchProfileDetails({ profileUrl, proxyUrl }) {
    const r = await fetchHttp(profileUrl, { proxyUrl, timeoutMillis: 20000 });
    if (r.statusCode !== 200) return null;
    if (isBlockedHtml(r.body)) return { blocked: true };

    const $ = cheerio.load(r.body);

    const email = ($('a[href^="mailto:"]').first().attr('href') || '').replace('mailto:', '').trim();

    // Keep selectors intentionally flexible; fields can be missing.
    const biography = (
        $('.biography, #biography, [class*="biograph"], [class*="bio"], .profile-description, .about-section')
            .first()
            .text()
            .trim() ||
        ''
    );

    const education = [];
    $('.education li, [class*="education"] li').each((_, el) => {
        const t = ($(el).text() || '').trim();
        if (t) education.push(t);
    });

    const barAdmissions = [];
    $('.admissions li, [class*="admission"] li, [class*="bar"] li').each((_, el) => {
        const t = ($(el).text() || '').trim();
        if (t) barAdmissions.push(t);
    });

    const languages = [];
    $('[class*="language"] li, .languages li').each((_, el) => {
        const t = ($(el).text() || '').trim();
        if (t) languages.push(t);
    });

    return {
        blocked: false,
        email: email || null,
        biography: biography || null,
        education: education.length ? education : null,
        barAdmissions: barAdmissions.length ? barAdmissions : null,
        languages: languages.length ? languages : null,
    };
}

async function maybeSaveDebugSnapshot({ key, url, statusCode, html, selectorCounts, apiUrlTried }) {
    await Actor.setValue(
        key,
        {
            url,
            statusCode,
            apiUrlTried: apiUrlTried || null,
            title: (cheerio.load(html || '')('title').text() || '').trim(),
            blocked: isBlockedHtml(html || ''),
            selectorCounts,
            htmlSnippet: (html || '').slice(0, 5000),
            createdAt: new Date().toISOString(),
        },
        { contentType: 'application/json' }
    );
}

async function runBrowserFallback({ startUrl, proxyConfiguration, maxLawyers, maxPages }) {
    log.warning('Falling back to browser mode (stealth) due to HTTP blocking or empty extraction');

    const seen = new Set();
    const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl() : undefined;

    const crawler = new PlaywrightCrawler({
        proxyConfiguration,
        maxRequestsPerCrawl: Math.min(20, Math.max(1, maxPages || 5)),
        maxConcurrency: 2,
        navigationTimeoutSecs: 45,
        requestHandlerTimeoutSecs: 120,
        launchContext: {
            launcher: firefox,
            launchOptions: await camoufoxLaunchOptions({
                headless: true,
                proxy: proxyUrl,
                geoip: true,
                os: 'windows',
                locale: 'en-US',
                screen: {
                    minWidth: 1024,
                    maxWidth: 1920,
                    minHeight: 768,
                    maxHeight: 1080,
                },
            }),
        },
        async requestHandler({ page, request, crawler: selfCrawler }) {
            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
            await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});

            const html = await page.content();
            
            // Try extraction methods in priority order
            let lawyers = [];
            
            // 1) Try embedded JSON first
            lawyers = extractListingViaEmbeddedJson({ html, pageUrl: request.url });
            
            // 2) Fallback to HTML parsing
            if (!lawyers.length) {
                lawyers = extractListingViaHtml({ html, pageUrl: request.url });
            }

            const filtered = lawyers.filter((l) => {
                if (!l.website) return false;
                if (seen.has(l.website)) return false;
                seen.add(l.website);
                return true;
            });

            const slice = maxLawyers > 0 ? filtered.slice(0, Math.max(0, maxLawyers - seen.size + filtered.length)) : filtered;
            if (slice.length) {
                await Actor.pushData(slice);
                log.info('Browser fallback saved lawyers', {
                    savedThisPage: slice.length,
                    totalScraped: seen.size,
                    url: request.url,
                });
            } else {
                log.warning('Browser fallback found no lawyers on page', {
                    url: request.url,
                    candidatesFound: lawyers.length,
                    lawyerLinks: cheerio.load(html)('a[href^="/lawyers/"]').length,
                });
            }

            if (maxLawyers > 0 && seen.size >= maxLawyers) return;

            const next = findNextPageUrl({ html, pageUrl: request.url });
            if (next) {
                await selfCrawler.addRequests([{ url: next, uniqueKey: next }]);
            }
        },
    });

    await crawler.run([startUrl]);
}

try {
    const input = (await Actor.getInput()) || {};

    const useSitemap = false;
    const debug = false;

    const maxLawyers = Number.isFinite(input.maxLawyers) ? input.maxLawyers : 50;
    const maxPages = Number.isFinite(input.maxPages) ? input.maxPages : 5;

    const proxyConfiguration = await Actor.createProxyConfiguration(
        input.proxyConfiguration || { useApifyProxy: true }
    );

    const searchUrl = (() => {
        if (input.searchUrl && input.searchUrl.trim()) return input.searchUrl.trim();
        const practiceArea = toSlug(input.practiceArea);
        const location = toSlug(input.location);
        if (!practiceArea || !location) {
            throw new Error('Invalid input: provide searchUrl OR both practiceArea and location');
        }
        return `${JUSTIA_BASE}/lawyers/${practiceArea}/${location}`;
    })();

    log.info('Starting Justia Lawyer Scraper', {
        searchUrl,
        maxLawyers,
        maxPages,
        fetchFullProfiles: !!input.fetchFullProfiles,
        useSitemap,
        debug,
    });

    const seenWebsites = new Set();
    let pagesProcessed = 0;
    let totalScraped = 0;
    let extractionMethod = 'HTTP';
    let consecutiveBlocked = 0;

    let nextUrl = searchUrl;

    while (nextUrl) {
        if (maxPages > 0 && pagesProcessed >= maxPages) break;
        if (maxLawyers > 0 && totalScraped >= maxLawyers) break;

        pagesProcessed += 1;

        const proxyUrl = await proxyConfiguration.newUrl().catch(() => undefined);
        const r = await fetchHttp(nextUrl, { proxyUrl, timeoutMillis: 25000 });

        const blocked = isBlockedHtml(r.body) || r.statusCode === 403 || r.statusCode === 429 || r.statusCode === 503;
        if (blocked) consecutiveBlocked += 1;
        else consecutiveBlocked = 0;

        if (blocked) {
            log.warning(`HTTP fetch looks blocked (status ${r.statusCode}), switching to browser fallback`, { url: nextUrl });
            // Immediately switch to browser fallback when blocked
            await runBrowserFallback({
                startUrl: nextUrl,
                proxyConfiguration,
                maxLawyers: maxLawyers > 0 ? Math.max(0, maxLawyers - totalScraped) : 0,
                maxPages: maxPages > 0 ? Math.max(0, maxPages - pagesProcessed) : 0,
            });
            break;
        }

        let lawyers = [];
        let apiUrlTried = '';

        // 1) JSON API (HTTP + JSON parse) — first priority
        if (!blocked) {
            const apiRes = await extractListingViaJsonApi({ pageUrl: r.url, html: r.body, proxyUrl });
            lawyers = apiRes.lawyers;
            apiUrlTried = apiRes.apiUrlTried;
            if (lawyers.length) extractionMethod = 'HTTP JSON API';
        }

        // 2) Embedded JSON (still JSON-first, no browser)
        if (!lawyers.length && !blocked) {
            lawyers = extractListingViaEmbeddedJson({ html: r.body, pageUrl: r.url });
            if (lawyers.length) extractionMethod = 'HTTP Embedded JSON';
        }

        // 3) HTML (HTTP + HTML parse) — second priority
        if (!lawyers.length && !blocked) {
            lawyers = extractListingViaHtml({ html: r.body, pageUrl: r.url });
            if (lawyers.length) extractionMethod = 'HTTP HTML';
        }

        const selectorCounts = {
            profileLinks: cheerio.load(r.body)('a[href^="/lawyers/"]').length,
        };

        if (debug && lawyers.length === 0) {
            await maybeSaveDebugSnapshot({
                key: `DEBUG_EMPTY_PAGE_${pagesProcessed}`,
                url: r.url,
                statusCode: r.statusCode,
                html: r.body,
                selectorCounts,
                apiUrlTried,
            });
        }

        // Save results
        if (lawyers.length) {
            const remaining = maxLawyers > 0 ? Math.max(0, maxLawyers - totalScraped) : lawyers.length;
            const unique = [];

            for (const l of lawyers) {
                if (!l.website) continue;
                if (seenWebsites.has(l.website)) continue;
                seenWebsites.add(l.website);
                unique.push(l);
                if (maxLawyers > 0 && unique.length >= remaining) break;
            }

            // Optional profile enrichment (HTTP)
            if (input.fetchFullProfiles && unique.length) {
                const enriched = [];
                for (const base of unique) {
                    const detailProxyUrl = await proxyConfiguration.newUrl().catch(() => undefined);
                    const details = await fetchProfileDetails({ profileUrl: base.website, proxyUrl: detailProxyUrl });

                    if (details?.blocked) {
                        enriched.push(base);
                        continue;
                    }

                    enriched.push({
                        ...base,
                        email: details?.email ?? base.email,
                        biography: details?.biography ?? base.biography,
                        education: details?.education ?? base.education,
                        barAdmissions: details?.barAdmissions ?? base.barAdmissions,
                        languages: details?.languages ?? base.languages,
                    });
                }
                await Actor.pushData(enriched);
                totalScraped += enriched.length;
            } else {
                await Actor.pushData(unique);
                totalScraped += unique.length;
            }

            log.info('Saved lawyers', {
                page: pagesProcessed,
                savedThisPage: unique.length,
                totalScraped,
                extractionMethod,
            });
        } else {
            log.warning('No lawyers extracted from page', {
                page: pagesProcessed,
                url: r.url,
                statusCode: r.statusCode,
                blocked,
                extractionMethod,
            });
        }

        if (maxLawyers > 0 && totalScraped >= maxLawyers) break;

        const next = findNextPageUrl({ html: r.body, pageUrl: r.url });
        nextUrl = next || '';


        if (!nextUrl) break;
    }

    const stats = {
        totalLawyersScraped: totalScraped,
        pagesProcessed,
        extractionMethod,
        timestamp: new Date().toISOString(),
    };

    await Actor.setValue('statistics', stats);
    log.info('Run finished', stats);

} catch (err) {
    log.exception(err, 'Actor failed');
    throw err;
} finally {
    await Actor.exit();
}
