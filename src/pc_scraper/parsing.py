"""HTML parsing for the target website's listing pages and product cards."""

from bs4 import BeautifulSoup, Tag
from loguru import logger

from pc_scraper.config import (
    PAGINATION_CLASS,
    PRODUCT_ATTRIBUTES_CLASS,
    PRODUCT_CONTAINER_CLASS,
    PRODUCT_NAME_CLASS,
    PRODUCT_PRICE_CLASS,
)


def parse_page_count(html: str) -> int:
    """Extract the total number of listing pages from a category page."""
    soup = BeautifulSoup(html, "lxml")
    elements = soup.find_all(class_=PAGINATION_CLASS)

    if not elements:
        raise ValueError(
            f"No pagination elements found (class='{PAGINATION_CLASS}')"
        )

    return max(int(el.get_text(strip=True)) for el in elements)


def parse_product(
    item: Tag, category_name: str, date: str
) -> dict[str, str] | None:
    """Parse a single product card. Returns None if the name is missing."""
    name_el = item.find(class_=PRODUCT_NAME_CLASS)

    if not name_el:
        logger.warning(
            f"[{category_name}] Skipping product - name element not found"
        )
        return None

    price_el = item.find(class_=PRODUCT_PRICE_CLASS)
    attributes_el = item.find(class_=PRODUCT_ATTRIBUTES_CLASS)

    return {
        "category": category_name,
        "date": date,
        "full_name": name_el.get_text(strip=True),
        "price": price_el.get_text(strip=True) if price_el else "n/a",
        "attributes": _extract_attributes_text(attributes_el),
    }


def _extract_attributes_text(attributes_el: Tag | None) -> str:
    """Return the attributes text for a product.

    The element's ``title`` holds a cleanly punctuated summary (e.g.
    "Memory: 16 GB, Chip: RTX 5070"); its child elements have no
    separators between them, so falling back to get_text() would
    concatenate everything into one unreadable run.
    """
    if attributes_el is None:
        return "n/a"

    title = attributes_el.get("title")
    if isinstance(title, str) and title:
        return title

    return attributes_el.get_text(strip=True)


def parse_listing_page(
    html: str, category_name: str, date: str
) -> list[dict[str, str]]:
    """Parse all products out of a single listing page."""
    soup = BeautifulSoup(html, "lxml")
    items = soup.find_all(class_=PRODUCT_CONTAINER_CLASS)

    if not items:
        logger.warning(f"[{category_name}] No products found on page")
        return []

    products = [parse_product(item, category_name, date) for item in items]
    return [product for product in products if product is not None]
