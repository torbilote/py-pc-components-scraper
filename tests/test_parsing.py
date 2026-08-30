import pytest
from bs4 import BeautifulSoup, Tag

from pc_scraper.parsing import (
    parse_listing_page,
    parse_page_count,
    parse_product,
)
from tests.conftest import (
    TEST_PAGINATION_CLASS,
    TEST_PRODUCT_ATTRIBUTES_CLASS,
    TEST_PRODUCT_CONTAINER_CLASS,
    TEST_PRODUCT_NAME_CLASS,
    TEST_PRODUCT_PRICE_CLASS,
)


def _product_card(html: str) -> Tag:
    item = BeautifulSoup(html, "lxml").find(
        class_=TEST_PRODUCT_CONTAINER_CLASS
    )
    assert isinstance(item, Tag)
    return item


def test_parse_product_extracts_all_fields() -> None:
    item = _product_card(
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_NAME_CLASS}">Test GPU</span>'
        f'<span class="{TEST_PRODUCT_PRICE_CLASS}">999 zl</span>'
        f'<span class="{TEST_PRODUCT_ATTRIBUTES_CLASS}">16GB</span>'
        "</div>"
    )

    assert parse_product(item, "gpu", "20260101") == {
        "category": "gpu",
        "date": "20260101",
        "full_name": "Test GPU",
        "price": "999 zl",
        "attributes": "16GB",
    }


def test_parse_product_returns_none_without_name() -> None:
    item = _product_card(
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_PRICE_CLASS}">999 zl</span></div>'
    )

    assert parse_product(item, "gpu", "20260101") is None


def test_parse_product_defaults_missing_optional_fields() -> None:
    item = _product_card(
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_NAME_CLASS}">Test GPU</span></div>'
    )

    product = parse_product(item, "gpu", "20260101")

    assert product is not None
    assert product["price"] == "n/a"
    assert product["attributes"] == "n/a"


def test_parse_product_uses_title_attribute_for_attributes_when_present() -> (
    None
):
    item = _product_card(
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_NAME_CLASS}">Test GPU</span>'
        f'<ul class="{TEST_PRODUCT_ATTRIBUTES_CLASS}" '
        f'title="Memory: 16 GB, Chip: RTX 5070">'
        f"<li>Memory:</li><li>16 GB</li><li>Chip:</li><li>RTX 5070</li>"
        f"</ul></div>"
    )

    product = parse_product(item, "gpu", "20260101")

    assert product is not None
    assert product["attributes"] == "Memory: 16 GB, Chip: RTX 5070"


def test_parse_product_falls_back_to_text_when_attributes_has_no_title() -> (
    None
):
    item = _product_card(
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_NAME_CLASS}">Test GPU</span>'
        f'<span class="{TEST_PRODUCT_ATTRIBUTES_CLASS}">16GB</span>'
        "</div>"
    )

    product = parse_product(item, "gpu", "20260101")

    assert product is not None
    assert product["attributes"] == "16GB"


def test_parse_listing_page_skips_products_without_name() -> None:
    html = (
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_NAME_CLASS}">A</span></div>'
        f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
        f'<span class="{TEST_PRODUCT_PRICE_CLASS}">no name</span></div>'
    )

    products = parse_listing_page(html, "gpu", "20260101")

    assert len(products) == 1
    assert products[0]["full_name"] == "A"


def test_parse_listing_page_returns_empty_list_when_no_products() -> None:
    assert parse_listing_page("<html></html>", "gpu", "20260101") == []


def test_parse_page_count_returns_highest_page_number() -> None:
    html = (
        f'<a class="{TEST_PAGINATION_CLASS}">1</a>'
        f'<a class="{TEST_PAGINATION_CLASS}">2</a>'
        f'<a class="{TEST_PAGINATION_CLASS}">7</a>'
    )

    assert parse_page_count(html) == 7


def test_parse_page_count_raises_when_pagination_missing() -> None:
    with pytest.raises(ValueError):
        parse_page_count("<html></html>")
