# py-pc-components-scraper

A command-line scraper that collects PC component prices and specifications
from a target website and saves the results as CSV files on your local
machine. It is triggered manually, has no external infrastructure
dependencies, and runs the same way locally or in Docker.

The scraped site's domain is intentionally not committed to this
repository — it is injected at runtime via the `TARGET_BASE_URL`
environment variable (see [Configuration](#configuration)).

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
TARGET_BASE_URL=
DATA_DIR=
```

| Variable                          | Required | Description                                                |
| ---------------------------------- | -------- | ------------------------------------------------------------ |
| `TARGET_BASE_URL`                 | **Yes**  | Base URL of the site to scrape (e.g. `https://example.com`)  |
| `DATA_DIR`                        | No       | Directory CSV output is written to (default: `./data`)      |
| `TARGET_PRODUCT_CONTAINER_CLASS`  | No       | CSS class of each product card on a listing page             |
| `TARGET_PRODUCT_NAME_CLASS`       | No       | CSS class of a product's name element                         |
| `TARGET_PRODUCT_PRICE_CLASS`      | No       | CSS class of a product's price element                        |
| `TARGET_PRODUCT_ATTRIBUTES_CLASS` | No       | CSS class of a product's attributes element                   |
| `TARGET_PAGINATION_CLASS`         | No       | CSS class of the pagination page-number elements               |

`TARGET_BASE_URL` has no default and is not committed anywhere in this
repository on purpose — the scraper refuses to start without it. Set it in
your local `.env` to the real site's base URL.

The target site's CSS class names are hashed and change whenever its
frontend is redeployed, which will eventually break scraping. Rather than
editing code and rebuilding the Docker image, set the `TARGET_*` class
variables above to the new class names (inspect the site's current markup
to find them) and rerun — no rebuild required.

## Usage

Run the scraper from the repository root:

```bash
uv run python -m pc_scraper
```

or, using the installed console script:

```bash
uv run pc-scraper
```

By default this scrapes every category. To scrape only specific ones, pass
their names to `--select`:

```bash
uv run python -m pc_scraper --select gpu cpu
```

Or scrape every category except some, with `--exclude`:

```bash
uv run python -m pc_scraper --exclude gpu cpu
```

Both can be combined, e.g. `--select gpu cpu --exclude ram`. Category
names must always follow one of these two flags — a bare category name
with no flag (e.g. `pc_scraper gpu`) is rejected.

Valid category names are listed in [What it scrapes](#what-it-scrapes); an
invalid one (in either list) is rejected with a usage error before any
requests are made.

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

To scrape specific categories (or exclude some), pass the same arguments
after the image name:

```bash
docker run --rm -v "$(pwd)/data:/app/data" pc-scraper --select gpu cpu
docker run --rm -v "$(pwd)/data:/app/data" pc-scraper --exclude gpu cpu
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
docker run --rm -e TARGET_PAGINATION_CLASS=newHash -v "$(pwd)/data:/app/data" pc-scraper

# 2. Forward a variable already set in your shell (name only, no "=value")
export TARGET_PAGINATION_CLASS=newHash
docker run --rm -e TARGET_PAGINATION_CLASS -v "$(pwd)/data:/app/data" pc-scraper

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
sleeps between pages and categories) to be considerate of the target
site's servers. Make sure your use complies with the site's terms of
service.
