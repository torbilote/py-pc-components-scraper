import cloudscraper


def create_scraper(proxies: list[str]) -> cloudscraper.CloudScraper:
    return cloudscraper.create_scraper(
        cookie_storage_dir="/tmp/cookies",
        rotating_proxies=proxies,
        circuit_failure_threshold=3,
        circuit_timeout=60,
    )
