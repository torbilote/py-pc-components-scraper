"""Shared pytest fixtures.

Tests use fixed, test-only CSS class names instead of the real target
website classes (which change on every site redeploy) so that updating a
production selector in config.py never breaks the unit test suite. The
same applies to TARGET_BASE_URL below: tests must not depend on the real
target site's domain, which is deliberately not committed to this repo.
"""

import os

os.environ.setdefault("TARGET_BASE_URL", "https://example.com")

import pytest  # noqa: E402

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
