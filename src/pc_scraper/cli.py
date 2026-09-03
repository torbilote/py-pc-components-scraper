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
    category_names = [category.name for category in CATEGORIES]
    parser = argparse.ArgumentParser(
        description="Scrape PC component listings from x-kom.pl."
    )
    parser.add_argument(
        "--select",
        nargs="+",
        default=[],
        choices=category_names,
        metavar="CATEGORY",
        help="Scrape only these categories (default: scrape every category).",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=[],
        choices=category_names,
        metavar="CATEGORY",
        help="Scrape every category except these.",
    )
    return parser.parse_args(argv)


def select_categories(
    include: list[str], exclude: list[str]
) -> list[Category]:
    """Return the categories to scrape.

    ``include`` narrows the base set to just the named categories, or
    every category if empty. ``exclude`` then removes any named
    categories from that set.
    """
    base = (
        CATEGORIES
        if not include
        else [category for category in CATEGORIES if category.name in include]
    )
    return [category for category in base if category.name not in exclude]


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    categories = select_categories(args.select, args.exclude)

    if not categories:
        logger.warning(
            "No categories to scrape after applying include/exclude filters"
        )
        return

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
