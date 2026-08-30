import time
from unittest.mock import Mock

import pytest

import pc_scraper.cli as cli_module
from pc_scraper.categories import Category
from pc_scraper.config import CATEGORY_SLEEP_SECONDS


def test_main_scrapes_and_saves_every_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(time, "sleep", Mock())

    scrape_category_mock = Mock(
        side_effect=[
            [{"full_name": "gpu-product"}],
            [{"full_name": "cpu-product"}],
        ]
    )
    monkeypatch.setattr(cli_module, "scrape_category", scrape_category_mock)
    save_products_csv_mock = Mock()
    monkeypatch.setattr(
        cli_module, "save_products_csv", save_products_csv_mock
    )

    cli_module.main([])

    assert scrape_category_mock.call_count == 2
    assert save_products_csv_mock.call_count == 2


def test_main_sleeps_between_categories_but_not_after_the_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(
        cli_module, "scrape_category", Mock(return_value=[{"x": "y"}])
    )
    monkeypatch.setattr(cli_module, "save_products_csv", Mock())
    sleep_mock = Mock()
    monkeypatch.setattr(time, "sleep", sleep_mock)

    cli_module.main([])

    sleep_mock.assert_called_once_with(CATEGORY_SLEEP_SECONDS)


def test_main_skips_save_when_no_products_scraped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(cli_module, "scrape_category", Mock(return_value=[]))
    save_products_csv_mock = Mock()
    monkeypatch.setattr(
        cli_module, "save_products_csv", save_products_csv_mock
    )

    cli_module.main([])

    save_products_csv_mock.assert_not_called()


def test_main_scrapes_only_the_requested_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(time, "sleep", Mock())

    scrape_category_mock = Mock(return_value=[{"full_name": "cpu-product"}])
    monkeypatch.setattr(cli_module, "scrape_category", scrape_category_mock)
    monkeypatch.setattr(cli_module, "save_products_csv", Mock())

    cli_module.main(["cpu"])

    assert scrape_category_mock.call_count == 1
    scraped_category = scrape_category_mock.call_args.args[1]
    assert scraped_category.name == "cpu"


def test_main_rejects_unknown_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.main(["not-a-real-category"])


def test_select_categories_returns_all_when_none_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories(None) == categories


def test_select_categories_returns_only_the_named_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories("cpu") == [categories[1]]


def test_parse_args_defaults_category_to_none() -> None:
    assert cli_module.parse_args([]).category is None


def test_parse_args_accepts_a_valid_category_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.parse_args(["gpu"]).category == "gpu"


def test_parse_args_rejects_an_invalid_category_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.parse_args(["not-a-real-category"])
