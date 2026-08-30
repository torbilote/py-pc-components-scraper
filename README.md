# py-pc-components-scraper

A command-line scraper that collects PC component prices and specifications
from [x-kom.pl](https://www.x-kom.pl) and saves the results as CSV files on
your local machine. It is triggered manually, has no external infrastructure
dependencies, and runs the same way locally or in Docker.

## What it scrapes

For each category below, the scraper walks every listing page and extracts
the product name, price, and key attributes:

| Category         | Category name   |
| ---------------- | --------------- |
| Graphics cards    | `gpu`           |
| Processors        | `cpu`           |
| SSD drives        | `ssd`           |
| HDD drives        | `hdd`           |
| RAM memory        | `ram`           |
| Motherboards      | `mobo`          |
| Liquid coolers    | `liquid_cooler` |
| Air coolers       | `air_cooler`    |
| Cases             | `case`          |

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) for dependency management
- Docker (optional), to run without installing Python locally

## Installation

```bash
uv sync
```

## Configuration

The scraper reads its configuration from environment variables (loaded from
a local `.env` file if present). Create one at the repository root:

```bash
DATA_DIR=
```

| Variable                        | Required | Description                                                 |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| `DATA_DIR`                       | No       | Directory CSV output is written to (default: `./data`)       |
| `XKOM_PRODUCT_CONTAINER_CLASS`   | No       | CSS class of each product card on a listing page              |
| `XKOM_PRODUCT_NAME_CLASS`        | No       | CSS class of a product's name element                          |
| `XKOM_PRODUCT_PRICE_CLASS`       | No       | CSS class of a product's price element                         |
| `XKOM_PRODUCT_ATTRIBUTES_CLASS`  | No       | CSS class of a product's attributes element                    |
| `XKOM_PAGINATION_CLASS`          | No       | CSS class of the pagination page-number elements                |

x-kom.pl's CSS class names are hashed and change whenever the site's
frontend is redeployed, which will eventually break scraping. Rather than
editing code and rebuilding the Docker image, set the `XKOM_*` variables
above to the new class names (inspect the site's current markup to find
them) and rerun — no rebuild required.

## Usage

Run the scraper from the repository root:

```bash
uv run python -m pc_scraper
```

or, using the installed console script:

```bash
uv run pc-scraper
```

By default this scrapes every category. To scrape just one, pass its name
as an argument:

```bash
uv run python -m pc_scraper gpu
```

Valid category names are listed in [What it scrapes](#what-it-scrapes); an
invalid one is rejected with a usage error before any requests are made.

Each run writes one CSV file per scraped category to:

```
data/<category>/<YYYYMMDD>.csv
```

Each row contains: `category`, `date`, `full_name`, `price`, `attributes`.

## Running with Docker

Build the image:

```bash
docker build -t pc-scraper .
```

Run it, mounting the `data` folder so results land on your host machine:

```bash
docker run --rm -v "$(pwd)/data:/app/data" pc-scraper
```

To scrape a single category, pass its name after the image name:

```bash
docker run --rm -v "$(pwd)/data:/app/data" pc-scraper gpu
```

> **Windows + Git Bash:** Git Bash auto-rewrites POSIX-style paths before
> Docker sees them, which can mangle the `host:container` volume syntax
> above (`$(pwd)` expands to `/c/...`) — in the worst case, this has been
> observed to *delete the local `data/` folder* rather than mount it.
> Prefix the command with `MSYS_NO_PATHCONV=1` to disable that rewriting:
> `MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd)/data:/app/data" pc-scraper`.
> PowerShell and cmd.exe are not affected.

Any variable from [Configuration](#configuration) can be passed in without
rebuilding the image, in any of three ways:

```bash
# 1. Explicit value
docker run --rm -e XKOM_PAGINATION_CLASS=newHash -v "$(pwd)/data:/app/data" pc-scraper

# 2. Forward a variable already set in your shell (name only, no "=value")
export XKOM_PAGINATION_CLASS=newHash
docker run --rm -e XKOM_PAGINATION_CLASS -v "$(pwd)/data:/app/data" pc-scraper

# 3. Load every variable from a file
docker run --rm --env-file .env -v "$(pwd)/data:/app/data" pc-scraper
```

## Development

```bash
uv run ruff check .       # lint
uv run ruff format .      # format
uv run mypy .             # type-check
uv run pytest             # tests
```

## Project structure

```
src/pc_scraper/
├── categories.py   # scraped categories and their listing URLs
├── cli.py          # entry point: orchestrates one full scraping run
├── config.py        # environment-driven configuration
├── http_client.py  # cloudscraper session factory
├── parsing.py       # HTML parsing for listing pages and product cards
├── scraping.py       # page-count discovery and per-category scraping
└── storage.py        # CSV output
tests/                # unit tests for every module above
```

## Notes

This project scrapes a third-party website. Requests are throttled (short
sleeps between pages and categories) to be considerate of x-kom.pl's
servers. Make sure your use complies with the site's terms of service.
