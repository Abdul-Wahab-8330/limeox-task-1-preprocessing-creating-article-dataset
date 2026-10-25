"""
News dataset collector with staged recovery.

Install Python packages:
    python -m pip install requests beautifulsoup4 feedparser tqdm playwright

Install the Playwright browser once:
    python -m playwright install chromium

Run:
    python build_news_dataset.py

The script collects every usable unique article it can find. It does not target
a fixed number of rows. Failed URLs are saved to failed_articles.json so the
unresolved failures can be inspected after the recovery attempts.
"""

import asyncio
import csv
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urldefrag

import feedparser
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError


# ============================================================
# PHASE 1 — SOURCES AND SETTINGS
# Add or update publisher RSS feeds here.
# ============================================================

MIN_CHARS = 300

# Number of regular HTTP requests running at the same time.
REQUEST_WORKERS = 12

# Number of Playwright pages working at the same time.
# Keep this lower than REQUEST_WORKERS because browsers use more memory.
BROWSER_WORKERS = 5

FEED_TIMEOUT_SECONDS = 15
ARTICLE_TIMEOUT_SECONDS = 20
RETRY_TIMEOUT_SECONDS = 35
BROWSER_TIMEOUT_MILLISECONDS = 30000

OUTPUT_FILE = "news_articles.csv"
REPORT_FILE = "article_collection_report.txt"
FAILED_FILE = "failed_articles.json"

FEEDS = {
    "The Guardian": [f"https://www.theguardian.com/{section}/rss" for section in [
        "world", "uk-news", "business", "technology", "science", "environment",
        "politics", "education", "culture", "sport", "us-news", "australia-news",
        "money", "society"
    ]],
    "BBC": [
        "https://feeds.bbci.co.uk/news/rss.xml",
        *[f"https://feeds.bbci.co.uk/news/{section}/rss.xml" for section in [
            "world", "uk", "business", "technology", "politics",
            "science_and_environment", "health", "education",
            "entertainment_and_arts"
        ]]
    ],
    "Deutsche Welle": [
        "https://rss.dw.com/rdf/rss-en-all",
        "https://rss.dw.com/rdf/rss-en-world",
        "https://rss.dw.com/rdf/rss-en-asia",
        "https://rss.dw.com/rdf/rss-en-africa",
        "https://rss.dw.com/rdf/rss-en-science",
        "https://rss.dw.com/rdf/rss-en-sports",
    ],
    "The Independent": ["https://www.independent.co.uk/news/world/rss"],
    "The News International": ["https://www.thenews.com.pk/rss/1/1"],
    "Fox News": [
        "https://moxie.foxnews.com/google-publisher/latest.xml",
        "https://moxie.foxnews.com/google-publisher/politics.xml",
    ],
    "Express Tribune": ["https://tribune.com.pk/feed/home"],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    )
}

COLUMNS = [
    "source", "url", "title", "content", "author",
    "published_at", "category", "collected_at"
]


# ============================================================
# SHARED HELPERS — used by the request and browser phases
# ============================================================

def get_meta(soup, *names):
    """Return the first non-empty value from common metadata tags."""
    for name in names:
        tag = soup.find("meta", attrs={"property": name})
        if tag is None:
            tag = soup.find("meta", attrs={"name": name})
        if tag and tag.get("content"):
            return tag["content"].strip()
    return ""


def clean_author(value):
    """Avoid treating an author-profile URL as an author's name."""
    value = (value or "").strip()
    if value.startswith(("https://", "http://", "/")):
        return ""
    return re.sub(r"\s+", " ", value)


def extract_article(html, final_url, item):
    """
    Extract a dataset record from HTML.
    Returns (record, None) on success or (None, reason) if content is unusable.
    """
    soup = BeautifulSoup(html, "html.parser")

    title = (
        get_meta(soup, "og:title", "twitter:title")
        or (soup.title.get_text(" ", strip=True) if soup.title else "")
        or item.get("title", "")
    )

        # Try <article> first, then <main>, then paragraphs from the whole page.
    areas = [
        soup.find("article"),
        soup.find("main"),
        soup
    ]

    content = ""

    for area in areas:
        if not area:
            continue

        # Remove elements that are not useful article content.
        for tag in area.select(
            "script, style, nav, footer, header, aside, form, noscript"
        ):
            tag.decompose()

        # Extract text from all non-empty paragraph tags.
        paragraphs = [
            p.get_text(" ", strip=True)
            for p in area.find_all("p")
            if p.get_text(" ", strip=True)
        ]

        candidate_content = "\n".join(paragraphs)

        # Accept this area only if it contains enough text.
        if len(candidate_content) >= MIN_CHARS:
            content = candidate_content
            break

    author = clean_author(
        get_meta(soup, "author", "article:author", "parsely-author")
        or item.get("author", "")
    )
    published = (
        get_meta(soup, "article:published_time", "datePublished", "pubdate")
        or item.get("published_at", "")
    )
    category = (
        get_meta(soup, "article:section", "section", "category")
        or item.get("category", "")
    )

    if not title.strip():
        return None, "missing_title"
    if len(content) < MIN_CHARS:
        return None, f"content_too_short_{len(content)}_chars"

    record = {
        "source": item["source"],
        "url": urldefrag(final_url)[0],
        "title": re.sub(r"\s+", " ", title).strip(),
        "content": content,
        "author": author or "Unknown",
        "published_at": published or "",
        "category": category or "Uncategorized",
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
    return record, None


def make_failure(item, stage, reason, attempts=1):
    """Keep enough information to retry or investigate this candidate later."""
    return {
        "source": item.get("source", "Unknown"),
        "url": urldefrag(item.get("url", ""))[0],
        "title": item.get("title", ""),
        "author": item.get("author", ""),
        "published_at": item.get("published_at", ""),
        "category": item.get("category", ""),
        "failure_stage": stage,
        "failure_reason": str(reason)[:500],
        "attempts": attempts,
    }


# ============================================================
# PHASE 2 — READ RSS FEEDS AND FETCH ARTICLES WITH REQUESTS
# First collect candidate links, then try normal HTTP requests.
# Every unsuccessful candidate is saved for the recovery phases.
# ============================================================

def read_feed(source, feed_url):
    """Read one RSS feed and return article candidates plus feed errors."""
    try:
        response = requests.get(
            feed_url, headers=HEADERS, timeout=FEED_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        feed = feedparser.parse(response.content)

        if not feed.entries and getattr(feed, "bozo", False):
            return [], [f"{feed_url}: feed parse error: {feed.bozo_exception}"]

        candidates = []
        for entry in feed.entries:
            url = entry.get("link", "")
            if url.startswith("http"):
                candidates.append({
                    "source": source,
                    "url": url,
                    "title": entry.get("title", ""),
                    "author": entry.get("author", ""),
                    "published_at": entry.get(
                        "published", entry.get("updated", "")
                    ),
                    "category": entry.get("category", ""),
                })
        return candidates, []
    except (requests.RequestException, ValueError) as error:
        return [], [f"{feed_url}: {type(error).__name__}: {error}"]


def fetch_with_requests(item, timeout=ARTICLE_TIMEOUT_SECONDS):
    """Try fetching and extracting one article using requests."""
    try:
        response = requests.get(
            item["url"], headers=HEADERS, timeout=timeout
        )
        response.raise_for_status()
        record, reason = extract_article(response.text, response.url, item)
        if record:
            return record, None
        return None, make_failure(item, "extraction", reason)
    except requests.RequestException as error:
        return None, make_failure(
            item, "request", f"{type(error).__name__}: {error}"
        )


def collect_candidates():
    """Fetch all feeds concurrently, deduplicate URLs, and retain feed errors."""
    feed_errors = []
    candidates = []

    feed_jobs = [
        (source, feed_url)
        for source, urls in FEEDS.items()
        for feed_url in urls
    ]

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as pool:
        futures = {
            pool.submit(read_feed, source, url): url
            for source, url in feed_jobs
        }
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Reading RSS feeds"
        ):
            try:
                found, errors = future.result()
                candidates.extend(found)
                feed_errors.extend(errors)
            except Exception as error:
                feed_errors.append(
                    f"{futures[future]}: unexpected error: {type(error).__name__}: {error}"
                )

    # A URL can appear in more than one feed; keep only its first occurrence.
    unique = {}
    for item in candidates:
        clean_url = urldefrag(item["url"])[0].rstrip("/")
        unique.setdefault(clean_url.lower(), item)

    return list(unique.values()), feed_errors


def fetch_candidates(candidates):
    """Fetch all candidate pages concurrently and return successes/failures."""
    articles = []
    failures = []

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as pool:
        futures = {
            pool.submit(fetch_with_requests, item): item
            for item in candidates
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Fetching article pages",
        ):
            try:
                record, failure = future.result()
                if record:
                    articles.append(record)
                elif failure:
                    failures.append(failure)
            except Exception as error:
                failures.append(make_failure(
                    futures[future], "unexpected",
                    f"{type(error).__name__}: {error}"
                ))

    return articles, failures


# ============================================================
# PHASE 3 — RETRY FAILED REQUESTS WITH A LONGER TIMEOUT
# Only retry request failures here. Extraction failures are saved
# for the browser phase because a longer timeout alone may not help.
# ============================================================

def is_transient_request_failure(failure):
    """
    Return True for failures worth retrying with a longer timeout.
    Do not waste a retry on permanent client errors such as HTTP 404.
    """
    if failure.get("failure_stage") != "request":
        return False

    reason = failure.get("failure_reason", "").lower()

    # Network timeouts and connection interruptions may be temporary.
    if "timeout" in reason or "connectionerror" in reason:
        return True

    # Retry rate limits and temporary server errors; do not retry 4xx errors
    # such as 403/404 in this timeout phase.
    return any(
        marker in reason
        for marker in [
            "429 client error",
            "500 server error",
            "502 server error",
            "503 server error",
            "504 server error",
            "500 internal server error",
            "502 bad gateway",
            "503 service unavailable",
            "504 gateway timeout",
        ]
    )


def retry_timeouts(failures):
    """Retry only transient request failures once with a longer timeout."""
    retry_items = [
        failure for failure in failures
        if is_transient_request_failure(failure)
    ]
    retry_urls = {item["url"] for item in retry_items}
    remaining = [
        failure for failure in failures
        if failure.get("url") not in retry_urls
    ]

    recovered = []
    still_failed = []

    with ThreadPoolExecutor(max_workers=REQUEST_WORKERS) as pool:
        futures = {
            pool.submit(
                fetch_with_requests, failure, RETRY_TIMEOUT_SECONDS
            ): failure
            for failure in retry_items
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Retrying failed requests",
        ):
            original = futures[future]
            try:
                record, failure = future.result()
                if record:
                    recovered.append(record)
                else:
                    failure["attempts"] = original.get("attempts", 1) + 1
                    still_failed.append(failure)
            except Exception as error:
                still_failed.append(make_failure(
                    original, "retry",
                    f"{type(error).__name__}: {error}",
                    original.get("attempts", 1) + 1,
                ))

    return recovered, remaining + still_failed


# ============================================================
# PHASE 4 — RECOVER REMAINING FAILURES WITH PLAYWRIGHT
# Use one Chromium browser with multiple concurrent pages.
# This is lighter than launching a separate browser process per URL.
# ============================================================

async def fetch_with_browser(browser, item, semaphore):
    """Open one page in Chromium and try to extract an article."""
    async with semaphore:
        page = None
        try:
            page = await browser.new_page(
                user_agent=HEADERS["User-Agent"]
            )
            response = await page.goto(
                item["url"],
                wait_until="domcontentloaded",
                timeout=BROWSER_TIMEOUT_MILLISECONDS,
            )

            # A non-success HTTP status is still recorded as a failure.
            if response is not None and response.status >= 400:
                return None, make_failure(
                    item, "browser_http",
                    f"Browser received HTTP {response.status}",
                    item.get("attempts", 1) + 1,
                )

            # Give client-side rendering a short chance to populate content.
            try:
                await page.locator("article p, main p, p").first.wait_for(
                    state="attached", timeout=5000
                )
            except PlaywrightTimeoutError:
                pass

            html = await page.content()
            final_url = page.url
            record, reason = extract_article(html, final_url, item)

            if record:
                return record, None
            return None, make_failure(
                item, "browser_extraction", reason,
                item.get("attempts", 1) + 1,
            )

        except Exception as error:
            return None, make_failure(
                item, "browser",
                f"{type(error).__name__}: {error}",
                item.get("attempts", 1) + 1,
            )
        finally:
            if page is not None:
                await page.close()


async def recover_with_playwright(failures):
    """Try unresolved failed candidates in parallel browser pages."""
    if not failures:
        return [], []

    recovered = []
    still_failed = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        semaphore = asyncio.Semaphore(BROWSER_WORKERS)
        tasks = [
            fetch_with_browser(browser, item, semaphore)
            for item in failures
        ]

        try:
            for task in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Recovering with Playwright",
            ):
                record, failure = await task
                if record:
                    recovered.append(record)
                elif failure:
                    still_failed.append(failure)
        finally:
            await browser.close()

    return recovered, still_failed


# ============================================================
# PHASE 5 — DEDUPLICATE, SAVE UNRESOLVED FAILURES, AND REPORT
# The CSV contains every unique article that passes validation.
# failed_articles.json contains only failures still unresolved.
# ============================================================

def deduplicate_articles(articles):
    """Deduplicate successful records by their final URL."""
    unique = {}
    for article in articles:
        key = urldefrag(article["url"])[0].rstrip("/").lower()
        unique.setdefault(key, article)
    return list(unique.values())


def save_failures(failures):
    """Overwrite the failure file with the current unresolved failures."""
    # Keep only one failure entry per candidate URL.
    unique = {}
    for failure in failures:
        key = urldefrag(failure.get("url", ""))[0].rstrip("/").lower()
        if key:
            unique[key] = failure

    with open(FAILED_FILE, "w", encoding="utf-8") as file:
        json.dump(list(unique.values()), file, ensure_ascii=False, indent=2)

    return list(unique.values())


def write_report(candidates, articles, failures, feed_errors, stage_counts):
    """Write counts by publisher and outcomes from each recovery phase."""
    source_counts = Counter(article["source"] for article in articles)
    failed_counts = Counter(item["source"] for item in failures)
    failure_reasons = Counter(item["failure_stage"] for item in failures)

    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("NEWS ARTICLE COLLECTION REPORT\n")
        report.write("=" * 32 + "\n\n")
        report.write(f"Unique RSS candidate URLs: {len(candidates)}\n")
        report.write(f"Final unique usable articles: {len(articles)}\n")
        report.write(f"Unresolved failed articles: {len(failures)}\n")
        report.write(f"RSS feed errors: {len(feed_errors)}\n\n")

        report.write("Recovery stage summary:\n")
        for stage, count in stage_counts.items():
            report.write(f"  {stage}: {count}\n")

        report.write("\nSuccessful articles by publisher:\n")
        for source in FEEDS:
            report.write(f"  {source}: {source_counts.get(source, 0)}\n")

        report.write("\nUnresolved failures by publisher:\n")
        for source in FEEDS:
            report.write(f"  {source}: {failed_counts.get(source, 0)}\n")

        report.write("\nUnresolved failures by stage:\n")
        for stage, count in sorted(failure_reasons.items()):
            report.write(f"  {stage}: {count}\n")

        if feed_errors:
            report.write("\nRSS feed errors (first 50):\n")
            for error in feed_errors[:50]:
                report.write(f"  - {error}\n")

        report.write(f"\nFailed article details: {FAILED_FILE}\n")
        report.write(f"Dataset CSV: {OUTPUT_FILE}\n")

    print("\nSuccessful articles by publisher:")
    for source in FEEDS:
        print(f"  {source}: {source_counts.get(source, 0)}")
    print(f"Final unique usable articles: {len(articles)}")
    print(f"Unresolved failures: {len(failures)}")
    print(f"Report saved to: {REPORT_FILE}")


def save_csv(articles):
    """Write all usable unique articles; there is no fixed 1,000-row target."""
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows({column: article.get(column, "") for column in COLUMNS}
                         for article in articles)
    print(f"CSV saved: {OUTPUT_FILE} ({len(articles)} articles)")


# ============================================================
# MAIN — RUN ALL PHASES IN ORDER
# ============================================================

async def main():
    print("PHASE 1: Source settings loaded.")
    print("PHASE 2: Collect RSS candidates and fetch pages with requests.")
    candidates, feed_errors = collect_candidates()
    print(f"Unique candidate URLs collected: {len(candidates)}")

    initial_articles, initial_failures = fetch_candidates(candidates)
    print(
        f"Initial request results: {len(initial_articles)} usable, "
        f"{len(initial_failures)} failed."
    )

    print("\nPHASE 3: Retry request failures once with a longer timeout.")
    timeout_recovered, after_timeout = retry_timeouts(initial_failures)

    print("\nPHASE 4: Try remaining failures using concurrent Playwright pages.")
    browser_recovered, unresolved = await recover_with_playwright(after_timeout)

    print("\nPHASE 5: Deduplicate, save CSV, failures, and report.")
    all_articles = deduplicate_articles(
        initial_articles + timeout_recovered + browser_recovered
    )
    unresolved = save_failures(unresolved)
    save_csv(all_articles)

    stage_counts = {
        "Initial request successes": len(initial_articles),
        "Recovered by longer-timeout retry": len(timeout_recovered),
        "Recovered by Playwright": len(browser_recovered),
        "Still unresolved": len(unresolved),
    }
    write_report(
        candidates, all_articles, unresolved, feed_errors, stage_counts
    )


if __name__ == "__main__":
    asyncio.run(main())
