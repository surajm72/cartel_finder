# Reddit Data Pipeline

A simplified data pipeline for processing Reddit data from JSON files into a PostgreSQL database.

## Overview

This pipeline consists of two main components:

1. **reddit_db_processor.py** - A standalone script that:
   - Sets up the database and creates tables if needed
   - Processes Reddit JSON files
   - Tracks processed files to avoid duplicates
   - Handles the hierarchical structure of posts, comments, and subcomments

2. **run_pipeline.py** - A script that orchestrates the pipeline:
   - Can run data collection (if available)
   - Processes the collected data into the database
   - Provides command-line options for flexibility

## Database Schema

The database schema consists of three main tables:

1. **reddit_posts** - Stores post details (title, body, metadata)
2. **reddit_comments** - Stores comments with references to posts
3. **reddit_subcomments** - Stores nested comments (replies to comments)

## Setup

### Prerequisites

- Python 3.8+
- PostgreSQL
- Required Python packages: `pip install -r requirements.txt`

### Configuration

Database connection parameters can be configured in a `.env` file:

```
DB_HOST=localhost
DB_PORT=5432
DB_USERNAME=postgres
DB_PASSWORD=your_password
DB_DATABASE=reddit_data
```

## Usage

### Running the Pipeline

To run the complete pipeline:

```bash
python run_pipeline.py --all
```

Options:
- `--collect` - Only run data collection
- `--process` - Only run data processing
- `--wait N` - Wait N seconds between collection and processing (default: 5)
- `--all` - Run all steps (default if no options specified)

### Direct Database Processing

To directly process JSON files into the database:

```bash
python reddit_db_processor.py
```

This will:
1. Set up the database if it doesn't exist
2. Create the necessary tables if they don't exist
3. Process all new JSON files in the `raw_data/reddit` directory
4. Update the processed files index

## File Structure

- `reddit_db_processor.py` - Main processing script
- `run_pipeline.py` - Pipeline orchestration
- `raw_data/reddit/` - Directory for Reddit JSON files
- `raw_data/reddit/processed_urls_index.json` - Tracks processed files
- `logs/` - Log files

## Extending the Pipeline

To add support for other data sources:
1. Create a new processor script for the data source
2. Update the run_pipeline.py to include the new processor 