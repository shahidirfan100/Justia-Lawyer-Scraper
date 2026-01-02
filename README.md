# Justia Lawyer Scraper

Extract lawyer and attorney profiles from Justia.com and export them as structured data.

This Actor is built for fast, production scraping with a resilient extraction strategy:

1. JSON endpoints (HTTP + JSON parse)
2. Embedded JSON data (HTTP + JSON parse)
3. HTML parsing (HTTP + HTML parse)
4. Browser fallback only when needed

## What it extracts

Each result can include (availability depends on the public profile):

- Name
- Firm name
- Practice areas
- Location / address
- Phone
- Email (if available)
- Profile URL
- Biography (optional)
- Education (optional)
- Bar admissions (optional)
- Languages (optional)
- Scraped timestamp

## Quick start

### Example: California criminal law

```json
{
  "practiceArea": "criminal-law",
  "location": "california",
  "maxLawyers": 50,
  "maxPages": 5,
  "fetchFullProfiles": false,
  "proxyConfiguration": { "useApifyProxy": true }
}
```

### Example: Use a direct URL

```json
{
  "searchUrl": "https://www.justia.com/lawyers/criminal-law/california",
  "maxLawyers": 50,
  "maxPages": 5,
  "fetchFullProfiles": false,
  "proxyConfiguration": { "useApifyProxy": true }
}
```

### Example: Enrich with full profiles

```json
{
  "searchUrl": "https://www.justia.com/lawyers/criminal-law/california",
  "maxLawyers": 50,
  "maxPages": 5,
  "fetchFullProfiles": true,
  "proxyConfiguration": { "useApifyProxy": true }
}
```

## Input

- `searchUrl` (string, optional)
  - If provided, the Actor scrapes this URL directly.
- `practiceArea` (string, required when `searchUrl` is not provided)
  - Use a slug format such as `criminal-law`, `family-law`, `personal-injury`.
- `location` (string, required when `searchUrl` is not provided)
  - Use a slug format such as `california`, `new-york`, `texas`.
- `maxLawyers` (number, default: `50`)
  - Maximum number of profiles to store. Use `0` for unlimited.
- `maxPages` (number, default: `5`)
  - Maximum number of listing pages to process. Use `0` for unlimited.
- `fetchFullProfiles` (boolean, default: `false`)
  - If enabled, the Actor visits each lawyer profile page and attempts to extract extra fields.
- `useSitemap` (boolean, default: `false`)
  - Reserved for sitemap-based discovery; if enabled, the Actor will attempt it and fall back automatically.
- `debug` (boolean, default: `false`)
  - If enabled, saves diagnostic snapshots to the key-value store when a page returns 0 extracted items.
- `proxyConfiguration` (object, recommended)
  - Use Apify Proxy for best reliability.

## Output

The Actor stores results in the default dataset.

## Tips for best results

- Use `proxyConfiguration` with Apify Proxy enabled.
- Keep `maxPages` and `maxLawyers` small while tuning inputs.
- Enable `debug` if you get 0 results to capture a snapshot for troubleshooting.

## Troubleshooting

### Getting 0 results

- Try setting `debug` to `true` and review the run’s key-value store records.
- Check that your `searchUrl` points to a Justia lawyer listing page.
- Ensure `practiceArea` and `location` are valid slugs if you are not using `searchUrl`.

### Runs are slow

- Disable `fetchFullProfiles`.
- Reduce `maxPages` and `maxLawyers`.

## Changelog

- 1.0: Production HTTP-first extraction with robust pagination and optional profile enrichment.
