import json
import math
from loguru import logger
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from bs4 import BeautifulSoup
import requests
 
HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9",
}

CATEGORIES: list[dict[str, str]] = [
    {
        "category_id": "gpu",
        "base_url": "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html",
        "pagination_selector": "eqRsIL",
    },
    {
        "category_id": "cpu",
        "base_url": "https://www.x-kom.pl/g-5/c/11-procesory.html",
        "pagination_selector":  "eqRsIL",
    },
]

# ── Krok 1: odczyt liczby stron ────────────────────────────────────────────────
 
def fetch_page_count(category: dict) -> int:
    url = category["base_url"]
    logger.info("[%s] Pobieram stronę 1: %s", category["category_id"], url)

    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
 
    soup = BeautifulSoup(response.text, "lxml")
    elements = soup.select(selector=category["pagination_selector"], limit=6)
    
    if not elements:
        raise ValueError(
            f"[{category['category_id']}] Nie znaleziono elementow paginacji "
            f"(selektor: '{category['pagination_selector']}'). "
            f"Sprawdź czy selektor CSS jest aktualny."
        )
    
    numbers  = [int(element.get_text(strip=True)) for element in elements]
    match = max(numbers)
 
    if not match:
        raise ValueError(
            f"[{category['category_id']}] Nie można znaleźć liczby stron w elementach paginacji. "
        )
 
    logger.info("[%s] Liczba stron: %d", category["category_id"], match)
    return match