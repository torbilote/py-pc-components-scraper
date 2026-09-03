from pc_scraper.categories import CATEGORIES
from pc_scraper.config import TARGET_BASE_URL


def test_category_urls_are_built_from_target_base_url() -> None:
    assert all(
        category.base_url.startswith(TARGET_BASE_URL)
        for category in CATEGORIES
    )


def test_every_category_name_is_unique() -> None:
    names = [category.name for category in CATEGORIES]
    assert len(names) == len(set(names))
