import csv
from datetime import datetime
from pathlib import Path

import pytest

import pc_scraper.storage as storage_module
from pc_scraper.storage import save_products_csv


def test_save_products_csv_writes_rows_matching_input(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(storage_module, "DATA_DIR", tmp_path)
    products = [
        {
            "category": "gpu",
            "date": "20260101",
            "full_name": "Test GPU",
            "price": "999 zl",
            "attributes": "16GB",
        }
    ]

    file_path = save_products_csv(
        "gpu", products, datetime(2026, 1, 1, 12, 30, 45)
    )

    with file_path.open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    assert rows == products


def test_save_products_csv_names_file_by_run_date(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(storage_module, "DATA_DIR", tmp_path)
    products = [
        {
            "category": "gpu",
            "date": "20260101",
            "full_name": "x",
            "price": "y",
            "attributes": "z",
        }
    ]

    file_path = save_products_csv(
        "gpu", products, datetime(2026, 1, 1, 12, 30, 45)
    )

    assert file_path == tmp_path / "gpu" / "20260101.csv"


def test_save_products_csv_creates_category_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(storage_module, "DATA_DIR", tmp_path)
    products = [
        {
            "category": "cpu",
            "date": "20260101",
            "full_name": "x",
            "price": "y",
            "attributes": "z",
        }
    ]

    file_path = save_products_csv("cpu", products, datetime(2026, 1, 1))

    assert file_path.parent == tmp_path / "cpu"
    assert file_path.parent.is_dir()


def test_save_products_csv_raises_for_empty_products(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(storage_module, "DATA_DIR", tmp_path)

    with pytest.raises(ValueError):
        save_products_csv("gpu", [], datetime(2026, 1, 1))
