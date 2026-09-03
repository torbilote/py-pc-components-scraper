import importlib
import os
from collections.abc import Generator, Iterator
from contextlib import contextmanager

import pytest

import pc_scraper.config as config_module

_CSS_CLASS_ENV_VARS = [
    ("TARGET_PRODUCT_CONTAINER_CLASS", "PRODUCT_CONTAINER_CLASS", "KWQcA"),
    ("TARGET_PRODUCT_NAME_CLASS", "PRODUCT_NAME_CLASS", "jNOIct"),
    ("TARGET_PRODUCT_PRICE_CLASS", "PRODUCT_PRICE_CLASS", "fDzLzF"),
    ("TARGET_PRODUCT_ATTRIBUTES_CLASS", "PRODUCT_ATTRIBUTES_CLASS", "iOXoLL"),
    ("TARGET_PAGINATION_CLASS", "PAGINATION_CLASS", "jTuhUe"),
]


@contextmanager
def _temporary_env(name: str, value: str | None) -> Generator[None]:
    """Set (or unset) an env var, restoring the original value afterwards."""
    original = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = original


@pytest.fixture(autouse=True)
def _restore_config_module() -> Iterator[None]:
    """Reload config after each test so later tests see the real env."""
    yield
    importlib.reload(config_module)


@pytest.mark.parametrize(("env_var", "attr", "default"), _CSS_CLASS_ENV_VARS)
def test_css_class_defaults_when_env_unset(
    env_var: str, attr: str, default: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A developer's local .env may set these too; ignore it here so this
    # test's result only depends on the process environment, not the
    # machine it happens to run on.
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: None)
    with _temporary_env(env_var, None):
        reloaded = importlib.reload(config_module)
        assert getattr(reloaded, attr) == default


@pytest.mark.parametrize(("env_var", "attr", "_default"), _CSS_CLASS_ENV_VARS)
def test_css_class_overridden_by_env(
    env_var: str, attr: str, _default: str
) -> None:
    with _temporary_env(env_var, "overriddenHash"):
        reloaded = importlib.reload(config_module)
        assert getattr(reloaded, attr) == "overriddenHash"


def test_target_base_url_read_from_env() -> None:
    with _temporary_env("TARGET_BASE_URL", "https://scraped-site.example"):
        reloaded = importlib.reload(config_module)
        assert reloaded.TARGET_BASE_URL == "https://scraped-site.example"


def test_target_base_url_strips_trailing_slash() -> None:
    with _temporary_env("TARGET_BASE_URL", "https://scraped-site.example/"):
        reloaded = importlib.reload(config_module)
        assert reloaded.TARGET_BASE_URL == "https://scraped-site.example"


def test_target_base_url_raises_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A developer's local .env may set this too; ignore it here so this
    # test's result only depends on the process environment.
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: None)
    with _temporary_env("TARGET_BASE_URL", None):
        with pytest.raises(RuntimeError, match="TARGET_BASE_URL"):
            importlib.reload(config_module)
