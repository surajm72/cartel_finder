# Reddit Data Scraper

This module is responsible for scraping Reddit posts and comments from legal-related subreddits.

## Features

- Configurable list of subreddits to scrape
- Scrapes post metadata (title, author, created date, upvote count, etc.)
- Scrapes post body content
- Scrapes the top 25 comments and their replies
- Anti-blocking measures (configurable delays, user agent rotation)
- Proxy support (configurable)
- Output to JSON files

## Configuration

All configuration options are in `project_root/config/reddit_config.py`. The main settings include:

- **SUBREDDITS**: List of subreddits to scrape
- **SCRAPING_CONFIG**: Settings for request delays, post limits, sort methods, etc.
- **OUTPUT_CONFIG**: Settings for output file format and location

## Running the Scraper

### Option 1: Run directly from the project root

```bash
cd project_root
python -m data_collection.reddit.run_scraper
```

### Option 2: Run with a specific subreddit

```bash
cd project_root
python -m data_collection.reddit.run_scraper --subreddit legaladvice
```

### Option 3: Run using Scrapy directly

```bash
cd project_root
scrapy crawl reddit -a subreddit=legaladvice
```

## Output

The scraper outputs JSON files to the `project_root/raw_data/reddit/` directory.

Each file is named according to the pattern in the config (default is `{subreddit}_{timestamp}.json`).

## Logs

Logs are written to the `project_root/logs/` directory.

## Adding New Subreddits

To add new subreddits, simply edit the `SUBREDDITS` list in `project_root/config/reddit_config.py`.

## Known Issues

- Reddit occasionally changes its HTML structure, which may break selectors
- The scraper uses old.reddit.com to maintain a more consistent structure
- Reddit may limit or block scraping if too many requests are made in a short time

## Next Steps

- Implement more robust anti-blocking measures
- Add support for scraping from the Reddit API via PRAW
- Improve comment threading to capture deeper comment chains 