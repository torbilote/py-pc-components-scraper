"""Shared pytest fixtures.

Tests use fixed, test-only CSS class names instead of the real x-kom.pl
classes (which change on every site redeploy) so that updating a
production selector in config.py never breaks the unit test suite.
"""

import pytest

TEST_PRODUCT_CONTAINER_CLASS = "test-product-container"
TEST_PRODUCT_NAME_CLASS = "test-product-name"
TEST_PRODUCT_PRICE_CLASS = "test-product-price"
TEST_PRODUCT_ATTRIBUTES_CLASS = "test-product-attributes"
TEST_PAGINATION_CLASS = "test-pagination"


@pytest.fixture(autouse=True)
def _stable_css_selectors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "pc_scraper.parsing.PRODUCT_CONTAINER_CLASS",
        TEST_PRODUCT_CONTAINER_CLASS,
    )
    monkeypatch.setattr(
        "pc_scraper.parsing.PRODUCT_NAME_CLASS", TEST_PRODUCT_NAME_CLASS
    )
    monkeypatch.setattr(
        "pc_scraper.parsing.PRODUCT_PRICE_CLASS", TEST_PRODUCT_PRICE_CLASS
    )
    monkeypatch.setattr(
        "pc_scraper.parsing.PRODUCT_ATTRIBUTES_CLASS",
        TEST_PRODUCT_ATTRIBUTES_CLASS,
    )
    monkeypatch.setattr(
        "pc_scraper.parsing.PAGINATION_CLASS", TEST_PAGINATION_CLASS
    )
