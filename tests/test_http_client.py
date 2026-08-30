from pathlib import Path
from typing import Any

import cloudscraper
import pytest

import pc_scraper.http_client as http_client_module
from pc_scraper.http_client import create_scraper


def test_create_scraper_configures_cookie_storage_and_stealth_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cookie_dir = tmp_path / "cookies"
    monkeypatch.setattr(http_client_module, "COOKIE_STORAGE_DIR", cookie_dir)

    captured_kwargs: dict[str, Any] = {}

    def fake_create_scraper(**kwargs: Any) -> str:
        captured_kwargs.update(kwargs)
        return "fake-scraper"

    monkeypatch.setattr(cloudscraper, "create_scraper", fake_create_scraper)

    result = create_scraper()

    assert result == "fake-scraper"
    assert captured_kwargs["cookie_storage_dir"] == str(cookie_dir)
    assert captured_kwargs["enable_stealth"] is True
    assert isinstance(captured_kwargs.get("stealth_options"), dict)


def test_create_scraper_creates_cookie_storage_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cookie_dir = tmp_path / "cookies"
    monkeypatch.setattr(http_client_module, "COOKIE_STORAGE_DIR", cookie_dir)
    monkeypatch.setattr(
        cloudscraper, "create_scraper", lambda **_: "fake-scraper"
    )

    assert not cookie_dir.exists()

    create_scraper()

    assert cookie_dir.is_dir()
