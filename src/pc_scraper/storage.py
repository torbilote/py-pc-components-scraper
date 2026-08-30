"""CSV output for scraped product data."""

import csv
from datetime import datetime
from pathlib import Path

from pc_scraper.config import DATA_DIR


def save_products_csv(
    category_name: str,
    products: list[dict[str, str]],
    run_started_at: datetime,
) -> Path:
    """Write scraped products to a timestamped CSV file under DATA_DIR."""
    if not products:
        raise ValueError(f"No products to save for category '{category_name}'")

    category_dir = DATA_DIR / category_name
    category_dir.mkdir(parents=True, exist_ok=True)

    file_path = category_dir / f"{run_started_at.strftime('%Y%m%d')}.csv"

    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file, fieldnames=products[0].keys(), quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(products)

    return file_path
