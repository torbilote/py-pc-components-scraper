def handler(event, context):
    import os
    from loguru import logger
    import cloudscraper
    from dotenv import load_dotenv
    load_dotenv()

    logger.info("Hello from Lambda!")

    proxies = [os.getenv(f"PROXY_URL_{i}", None) for i in range(1, 2)]
    
    logger.info(f"Proxies: {','.join([f'{proxy[:20]}...' for proxy in proxies if proxy])}")

    scraper = cloudscraper.create_scraper(
        cookie_storage_dir="/tmp/cookies",
        rotating_proxies=proxies if proxies else None,
        circuit_failure_threshold=3,
        circuit_timeout=60,
        # interpreter='hybrid'
        )

    # url = 'http://google.pl/'
    url = 'https://realpython.com/'
    # url = 'https://x-kom.pl/'

    response = scraper.get(url)
    
    logger.info(response.url)
    logger.info(response.status_code)

    return "ok"

if __name__ == "__main__":
    handler(None, None)