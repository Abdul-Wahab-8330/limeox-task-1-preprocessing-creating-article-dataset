# News dataset collector — run guide

## What the script does

The script attempts to collect as many usable unique articles as possible from the seven configured publishers. It no longer stops at 1,000 rows.

### Phase 1 — Sources and settings
Publisher RSS feeds, timeouts, worker counts, minimum article length, and output filenames are kept at the top of `build_news_dataset.py`.

### Phase 2 — RSS and regular HTTP requests
RSS feeds are read concurrently. Article URLs are deduplicated, then downloaded with `requests`. Articles need a title and at least 300 characters of paragraph text to pass validation. Progress bars use `tqdm`.

### Phase 3 — Longer-timeout retry
Request failures are tried one more time with a longer timeout. Extraction failures are passed on to the browser phase instead of being retried pointlessly with the same extraction method.

### Phase 4 — Playwright recovery
The script launches one headless Chromium browser and opens multiple pages concurrently. This is usually lighter than launching a separate browser process for every URL. Browser automation may recover JavaScript-rendered pages, but it cannot guarantee access to every site; HTTP 403s and publisher restrictions may remain.

### Phase 5 — Output
- `news_articles.csv`: every unique article that passed validation, regardless of the total count.
- `failed_articles.json`: only the unresolved failures after all recovery attempts. It includes URL, publisher, failure stage/reason, and attempt count.
- `article_collection_report.txt`: candidate count, successful article counts by publisher, unresolved failures by publisher and stage, recovery counts, and RSS feed errors.

The failure JSON is rewritten each run with the failures from that run. If you want to preserve historical failure records across runs, make a dated backup before running the script again.

## Install

Activate your project virtual environment, then run:

```bash
python -m pip install requests beautifulsoup4 feedparser tqdm playwright
python -m playwright install chromium
```

## Run

Save the script as `build_news_dataset.py` in your project folder, then run:

```bash
python build_news_dataset.py
```

## Adjusting speed

Start with `REQUEST_WORKERS = 12` and `BROWSER_WORKERS = 5`. Browser pages use more memory than normal requests, so avoid setting browser concurrency extremely high. If your machine runs out of memory or publishers start returning more errors, lower the worker values.

## Notes

- The script does not bypass paywalls, CAPTCHAs, logins, or access restrictions.
- The CSV only includes pages that pass the title/content validation. Missing authors are labeled `Unknown`; missing categories are labeled `Uncategorized`.
- If a feed fails, its error is included in the report. A feed with no entries may be empty or may have changed format.
