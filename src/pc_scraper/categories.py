"""Product categories scraped from x-kom.pl, and their listing page URLs."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Category:
    name: str
    base_url: str


CATEGORIES: list[Category] = [
    Category("gpu", "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html"),
    Category("cpu", "https://www.x-kom.pl/g-5/c/11-procesory.html"),
    Category("ssd", "https://www.x-kom.pl/g-5/c/1779-dyski-ssd.html"),
    Category("hdd", "https://www.x-kom.pl/g-5/c/1580-dyski-hdd.html"),
    Category("ram", "https://www.x-kom.pl/g-5/c/28-pamieci-ram.html"),
    Category("mobo", "https://www.x-kom.pl/g-5/c/14-plyty-glowne.html"),
    Category(
        "liquid_cooler",
        "https://www.x-kom.pl/g-5/c/2650-chlodzenia-wodne.html",
    ),
    Category(
        "air_cooler",
        "https://www.x-kom.pl/g-5/c/105-chlodzenia-procesorow.html",
    ),
    Category(
        "case", "https://www.x-kom.pl/g-5/c/389-obudowy-do-komputera.html"
    ),
]
