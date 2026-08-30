"""Entry point: scrape one or all categories and save results locally."""

import argparse
import time
from datetime import datetime

from loguru import logger

from pc_scraper.categories import CATEGORIES, Category
from pc_scraper.config import CATEGORY_SLEEP_SECONDS
from pc_scraper.http_client import create_scraper
from pc_scraper.scraping import scrape_category
from pc_scraper.storage import save_products_csv


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape PC component listings from x-kom.pl."
    )
    parser.add_argument(
        "category",
        nargs="?",
        default=None,
        choices=[category.name for category in CATEGORIES],
        help="Scrape only this category (default: scrape every category).",
    )
    return parser.parse_args(argv)


def select_categories(category_name: str | None) -> list[Category]:
    """Return the categories to scrape: all of them, or just the named one."""
    if category_name is None:
        return CATEGORIES
    return [
        category for category in CATEGORIES if category.name == category_name
    ]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    categories = select_categories(args.category)

    logger.info("Starting py-pc-components-scraper")

    scraper = create_scraper()
    run_started_at = datetime.now()
    date = run_started_at.strftime("%Y%m%d")

    for index, category in enumerate(categories, start=1):
        logger.info(f"Processing category: {category.name}")
        products = scrape_category(scraper, category, date)

        if not products:
            logger.error(
                f"[{category.name}] No products scraped, skipping save"
            )
        else:
            file_path = save_products_csv(
                category.name, products, run_started_at
            )
            logger.info(
                f"[{category.name}] Saved {len(products)} products to {file_path}"
            )

        if index < len(categories):
            time.sleep(CATEGORY_SLEEP_SECONDS)

    logger.info("Scraping run complete")
