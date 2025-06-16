# Legal Data Pipeline

A comprehensive data pipeline for collecting, processing, and analyzing legal data from various sources including Reddit, CFPB, SEC filings, and news articles.

## Project Structure

```
project_root/
├── data_collection/     # Scripts for scraping/collecting data from sources
│   ├── reddit/          # Reddit data collection components
│   ├── cfpb/            # Consumer Financial Protection Bureau data collectors
│   ├── sec/             # SEC filings data collection
│   └── news/            # News articles data collection
├── raw_data/            # Storage for raw, unprocessed data files
│   ├── reddit/
│   ├── cfpb/
│   ├── sec/
│   └── news/
├── data_pipeline/       # Processing, transformation, and cleaning scripts
│   ├── reddit/
│   ├── cfpb/
│   ├── sec/
│   └── news/
├── processed_data/      # Clean, structured data ready for database loading
│   ├── reddit/
│   ├── cfpb/
│   ├── sec/
│   └── news/
├── database/            # Database connection, schemas, and loading scripts
├── config/              # Configuration files for different environments
│   ├── reddit_config.py      # Reddit scraping configuration
│   └── database_config.py    # Database connection configuration
├── config.ini           # External configuration file for easy settings management
├── utils/               # Shared utility functions
└── orchestration/       # Pipeline orchestration (Airflow DAGs, etc.)
```

## Technology Stack

- **Scrapy** - Main scraping framework for Reddit, CFPB, and news sources
- **Playwright** - For JavaScript-heavy websites like SEC filings
- **Pandas** - Data manipulation and analysis
- **Postgresql** - Data storage

## Configuration

The system uses a `config.ini` file in the root directory for external configuration without modifying code:

```ini
[database]
host=localhost
port=5432
username=postgres
password=password
database=reddit_data

[logging]
level=INFO
file=logs/reddit_processor.log

[scraping]
request_delay=2.0
retries=3
```

You can modify these settings without touching any Python code.

## Getting Started

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure settings in `config.ini` file

3. Run the pipeline:
   ```
   # Run all pipeline steps
   python run_pipeline.py --all
   
   # Or run individual steps
   python run_pipeline.py --collect  # Only collect data
   python run_pipeline.py --process  # Only process collected data
   ```

## Features

- Configurable subreddit list for Reddit scraping
- Comment scraping in a flat structure (no nested subcomments)
- Anti-blocking mechanisms with configurable delays
- Proxy support (configurable)
- Data validation and cleaning pipelines
- Modular architecture for easy addition of new data sources
- External configuration via config.ini

## Data Pipeline

The data pipeline processes raw data collected by the scrapers and loads it into a PostgreSQL database. 

### Features

- **Database Schema**: Structured database for Reddit posts and comments
- **File Tracking**: Keeps track of processed files to avoid duplicate processing
- **Incremental Loading**: Only processes new data files
- **Robust Error Handling**: Logs errors and continues processing
- **External Configuration**: Database settings in config.ini

### Data Model

The database schema consists of two main tables:

1. **RedditPost** - Stores all post details
2. **RedditComment** - Stores all comments related to posts (including replies)

This structure simplifies the data model while preserving the relationship between posts and comments.
