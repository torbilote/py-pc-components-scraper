"""Product categories scraped from the target website, and their URLs."""

from dataclasses import dataclass

from pc_scraper.config import TARGET_BASE_URL


@dataclass(frozen=True)
class Category:
    name: str
    base_url: str


_CATEGORY_PATHS: list[tuple[str, str]] = [
    ("gpu", "/g-5/c/345-karty-graficzne.html"),
    ("cpu", "/g-5/c/11-procesory.html"),
    ("ssd", "/g-5/c/1779-dyski-ssd.html"),
    ("hdd", "/g-5/c/1580-dyski-hdd.html"),
    ("ram", "/g-5/c/28-pamieci-ram.html"),
    ("mobo", "/g-5/c/14-plyty-glowne.html"),
    ("liquid_cooler", "/g-5/c/2650-chlodzenia-wodne.html"),
    ("air_cooler", "/g-5/c/105-chlodzenia-procesorow.html"),
    ("case", "/g-5/c/389-obudowy-do-komputera.html"),
]

CATEGORIES: list[Category] = [
    Category(name, f"{TARGET_BASE_URL}{path}")
    for name, path in _CATEGORY_PATHS
]
