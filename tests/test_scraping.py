from unittest.mock import Mock

import pytest
from requests import HTTPError

from pc_scraper.categories import Category
from pc_scraper.config import MAX_PAGE_FETCH_ATTEMPTS, RETRY_BACKOFF_SECONDS
from pc_scraper.scraping import (
    fetch_page_count,
    generate_page_urls,
    scrape_category,
    scrape_page,
)
from tests.conftest import (
    TEST_PAGINATION_CLASS,
    TEST_PRODUCT_CONTAINER_CLASS,
    TEST_PRODUCT_NAME_CLASS,
)

_PAGE_HTML = (
    f'<div class="{TEST_PRODUCT_CONTAINER_CLASS}">'
    f'<span class="{TEST_PRODUCT_NAME_CLASS}">A</span></div>'
)
_PAGINATION_HTML = (
    f'<a class="{TEST_PAGINATION_CLASS}">1</a>'
    f'<a class="{TEST_PAGINATION_CLASS}">2</a>'
)


def _fake_response(text: str, *, ok: bool = True) -> Mock:
    response = Mock()
    response.text = text
    response.raise_for_status = Mock(
        side_effect=None if ok else HTTPError("boom")
    )
    return response


def test_generate_page_urls_builds_sequential_pages() -> None:
    urls = generate_page_urls("https://example.com/cat.html", 3)

    assert urls == [
        "https://example.com/cat.html?page=1",
        "https://example.com/cat.html?page=2",
        "https://example.com/cat.html?page=3",
    ]


def test_generate_page_urls_handles_single_page() -> None:
    assert generate_page_urls("https://example.com/cat.html", 1) == [
        "https://example.com/cat.html?page=1"
    ]


def test_fetch_page_count_parses_pagination_from_response() -> None:
    html = (
        f'<a class="{TEST_PAGINATION_CLASS}">1</a>'
        f'<a class="{TEST_PAGINATION_CLASS}">3</a>'
    )
    scraper = Mock()
    scraper.get.return_value = _fake_response(html)

    page_count = fetch_page_count(scraper, "https://example.com/cat.html")

    assert page_count == 3
    scraper.get.assert_called_once_with(
        "https://example.com/cat.html", timeout=30.0
    )


def test_fetch_page_count_raises_on_http_error() -> None:
    scraper = Mock()
    scraper.get.return_value = _fake_response("", ok=False)

    with pytest.raises(HTTPError):
        fetch_page_count(scraper, "https://example.com/cat.html")


def test_scrape_page_returns_parsed_products() -> None:
    scraper = Mock()
    scraper.get.return_value = _fake_response(_PAGE_HTML)

    products = scrape_page(
        scraper, "https://example.com/p1", "gpu", "20260101"
    )

    assert products == [
        {
            "category": "gpu",
            "date": "20260101",
            "full_name": "A",
            "price": "n/a",
            "attributes": "n/a",
        }
    ]


def test_scrape_page_raises_on_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", lambda *_: None)
    scraper = Mock()
    scraper.get.return_value = _fake_response("", ok=False)

    with pytest.raises(HTTPError):
        scrape_page(scraper, "https://example.com/p1", "gpu", "20260101")


def test_scrape_page_retries_transient_http_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", lambda *_: None)
    scraper = Mock()
    scraper.get.side_effect = [
        _fake_response("", ok=False),
        _fake_response(_PAGE_HTML),
    ]

    products = scrape_page(
        scraper, "https://example.com/p1", "gpu", "20260101"
    )

    assert len(products) == 1
    assert scraper.get.call_count == 2


def test_scrape_page_raises_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", lambda *_: None)
    scraper = Mock()
    scraper.get.return_value = _fake_response("", ok=False)

    with pytest.raises(HTTPError):
        scrape_page(scraper, "https://example.com/p1", "gpu", "20260101")

    assert scraper.get.call_count == MAX_PAGE_FETCH_ATTEMPTS


def test_scrape_page_sleeps_only_between_retry_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_mock = Mock()
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", sleep_mock)
    scraper = Mock()
    scraper.get.side_effect = [
        _fake_response("", ok=False),
        _fake_response(_PAGE_HTML),
    ]

    scrape_page(scraper, "https://example.com/p1", "gpu", "20260101")

    sleep_mock.assert_called_once_with(RETRY_BACKOFF_SECONDS)


def test_scrape_category_aggregates_products_across_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", lambda *_: None)

    category = Category("gpu", "https://example.com/cat.html")
    scraper = Mock()
    scraper.get.side_effect = [
        _fake_response(_PAGINATION_HTML),
        _fake_response(_PAGE_HTML),
        _fake_response(_PAGE_HTML),
    ]

    products = scrape_category(scraper, category, "20260101")

    assert len(products) == 2
    assert scraper.get.call_count == 3


def test_scrape_category_skips_pages_that_persistently_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", lambda *_: None)

    category = Category("gpu", "https://example.com/cat.html")
    scraper = Mock()
    scraper.get.side_effect = [
        _fake_response(_PAGINATION_HTML),
        *[_fake_response("", ok=False)] * MAX_PAGE_FETCH_ATTEMPTS,
        _fake_response(_PAGE_HTML),
    ]

    products = scrape_category(scraper, category, "20260101")

    assert len(products) == 1
    assert scraper.get.call_count == 1 + MAX_PAGE_FETCH_ATTEMPTS + 1


def test_scrape_category_sleeps_between_pages_but_not_after_the_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_mock = Mock()
    monkeypatch.setattr("pc_scraper.scraping.time.sleep", sleep_mock)

    category = Category("gpu", "https://example.com/cat.html")
    scraper = Mock()
    scraper.get.side_effect = [
        _fake_response(_PAGINATION_HTML),
        _fake_response(_PAGE_HTML),
        _fake_response(_PAGE_HTML),
    ]

    scrape_category(scraper, category, "20260101")

    assert sleep_mock.call_count == 2
