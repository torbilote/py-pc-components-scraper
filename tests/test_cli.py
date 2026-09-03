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

    cli_module.main(["--select", "cpu"])

    assert scrape_category_mock.call_count == 1
    scraped_category = scrape_category_mock.call_args.args[1]
    assert scraped_category.name == "cpu"


def test_main_scrapes_multiple_requested_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
        Category("ssd", "https://example.com/ssd.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(time, "sleep", Mock())

    scrape_category_mock = Mock(return_value=[{"full_name": "x"}])
    monkeypatch.setattr(cli_module, "scrape_category", scrape_category_mock)
    monkeypatch.setattr(cli_module, "save_products_csv", Mock())

    cli_module.main(["--select", "gpu", "ssd"])

    scraped_names = {
        call.args[1].name for call in scrape_category_mock.call_args_list
    }
    assert scraped_names == {"gpu", "ssd"}


def test_main_excludes_requested_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
        Category("ssd", "https://example.com/ssd.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)
    monkeypatch.setattr(cli_module, "create_scraper", lambda: "fake-scraper")
    monkeypatch.setattr(time, "sleep", Mock())

    scrape_category_mock = Mock(return_value=[{"full_name": "x"}])
    monkeypatch.setattr(cli_module, "scrape_category", scrape_category_mock)
    monkeypatch.setattr(cli_module, "save_products_csv", Mock())

    cli_module.main(["--exclude", "cpu"])

    scraped_names = {
        call.args[1].name for call in scrape_category_mock.call_args_list
    }
    assert scraped_names == {"gpu", "ssd"}


def test_main_rejects_unknown_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.main(["--select", "not-a-real-category"])


def test_main_rejects_a_category_without_the_flag_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--categories must be spelled out; a bare category name is rejected."""
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.main(["gpu"])


def test_main_rejects_unknown_excluded_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.main(["--exclude", "not-a-real-category"])


def test_select_categories_returns_all_when_no_filters_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories([], []) == categories


def test_select_categories_returns_only_included_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
        Category("ssd", "https://example.com/ssd.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories(["gpu", "ssd"], []) == [
        categories[0],
        categories[2],
    ]


def test_select_categories_removes_excluded_categories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
        Category("ssd", "https://example.com/ssd.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories([], ["cpu"]) == [
        categories[0],
        categories[2],
    ]


def test_select_categories_applies_include_then_exclude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
        Category("ssd", "https://example.com/ssd.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    assert cli_module.select_categories(["gpu", "cpu"], ["cpu"]) == [
        categories[0]
    ]


def test_parse_args_defaults_categories_and_exclude_to_empty_lists() -> None:
    args = cli_module.parse_args([])
    assert args.select == []
    assert args.exclude == []


def test_parse_args_accepts_multiple_category_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    args = cli_module.parse_args(["--select", "gpu", "cpu"])

    assert args.select == ["gpu", "cpu"]
    assert args.exclude == []


def test_parse_args_accepts_exclude_flag_with_multiple_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [
        Category("gpu", "https://example.com/gpu.html"),
        Category("cpu", "https://example.com/cpu.html"),
    ]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    args = cli_module.parse_args(["--exclude", "gpu", "cpu"])

    assert args.select == []
    assert args.exclude == ["gpu", "cpu"]


def test_parse_args_rejects_an_invalid_category_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.parse_args(["--select", "not-a-real-category"])


def test_parse_args_rejects_a_category_without_the_flag_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.parse_args(["gpu"])


def test_parse_args_rejects_an_invalid_excluded_category_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    categories = [Category("gpu", "https://example.com/gpu.html")]
    monkeypatch.setattr(cli_module, "CATEGORIES", categories)

    with pytest.raises(SystemExit):
        cli_module.parse_args(["--exclude", "not-a-real-category"])
