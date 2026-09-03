"""Per-category scraping: page discovery, fetching, and parsing."""

import time
from typing import cast

from cloudscraper import CloudScraper
from loguru import logger
from requests import HTTPError

from pc_scraper.categories import Category
from pc_scraper.config import (
    MAX_PAGE_FETCH_ATTEMPTS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_SECONDS,
    URL_SLEEP_SECONDS,
)
from pc_scraper.parsing import parse_listing_page, parse_page_count


def _fetch_html_with_retries(
    scraper: CloudScraper, url: str, category_name: str
) -> str:
    """Fetch a URL's HTML, retrying a bounded number of times on HTTP errors.

    The target website occasionally returns a transient error (e.g. a 404) for
    an otherwise-valid page; retrying avoids losing data to a one-off blip.
    """
    attempt = 1
    while True:
        try:
            response = scraper.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
        except HTTPError:
            if attempt >= MAX_PAGE_FETCH_ATTEMPTS:
                raise
            logger.warning(
                f"[{category_name}] Attempt {attempt}/"
                f"{MAX_PAGE_FETCH_ATTEMPTS} failed for {url}, retrying"
            )
            time.sleep(RETRY_BACKOFF_SECONDS)
            attempt += 1
        else:
            return cast(str, response.text)


def fetch_page_count(
    scraper: CloudScraper, url: str, category_name: str
) -> int:
    """Fetch a category's base listing page and return its total page count."""
    html = _fetch_html_with_retries(scraper, url, category_name)
    return parse_page_count(html)


def generate_page_urls(base_url: str, page_count: int) -> list[str]:
    """Generate paginated listing URLs for all pages of a category."""
    return [f"{base_url}?page={i}" for i in range(1, page_count + 1)]


def scrape_page(
    scraper: CloudScraper, url: str, category_name: str, date: str
) -> list[dict[str, str]]:
    """Fetch and parse a single listing page."""
    html = _fetch_html_with_retries(scraper, url, category_name)
    products = parse_listing_page(html, category_name, date)
    logger.info(f"[{category_name}] {url} -> {len(products)} products parsed")
    return products


def scrape_category(
    scraper: CloudScraper, category: Category, date: str
) -> list[dict[str, str]]:
    """Scrape every listing page for a single category."""
    logger.info(f"[{category.name}] Fetching page count")
    page_count = fetch_page_count(scraper, category.base_url, category.name)
    urls = generate_page_urls(category.base_url, page_count)
    logger.info(f"[{category.name}] {page_count} pages found")

    all_products: list[dict[str, str]] = []

    for index, url in enumerate(urls, start=1):
        logger.info(
            f"[{category.name}] Scraping page {index}/{len(urls)}: {url}"
        )

        try:
            all_products.extend(scrape_page(scraper, url, category.name, date))
        except (HTTPError, AttributeError, ValueError) as exc:
            logger.error(f"[{category.name}] Failed to scrape {url}: {exc}")

        time.sleep(URL_SLEEP_SECONDS)

    logger.info(
        f"[{category.name}] Scraped {len(all_products)} products across "
        f"{len(urls)} pages"
    )
    return all_products
