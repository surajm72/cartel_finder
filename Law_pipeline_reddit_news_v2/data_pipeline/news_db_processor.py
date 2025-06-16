#!/usr/bin/env python
"""
News data processor for handling data from multiple news sources.
This script handles both database setup and data processing in one file.
"""
import os
import sys
import json
import logging
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from config.database_config import DB_CONFIG, get_db_connection, ensure_database_exists
from config.news_config import NEWS_SOURCES, OUTPUT_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/news_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("news_processor")

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# SQL for creating tables
CREATE_TABLES_SQL = """
-- News articles table
CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    url VARCHAR UNIQUE,
    title VARCHAR,
    author VARCHAR,
    published_date TIMESTAMP,
    description TEXT,
    body TEXT,
    source VARCHAR,
    scraped_at TIMESTAMP,
    file_source VARCHAR,
    processed_at TIMESTAMP,
    tags TEXT[]  -- Store tags as an array
);
"""

# Path to news raw data files
NEWS_DATA_DIR = OUTPUT_CONFIG['output_folder']
# Path to processed files index
PROCESSED_FILES_INDEX = os.path.join("data_pipeline", "processed_news_files_index.json")

def setup_database():
    """
    Set up the database tables.
    
    Returns:
        bool: True if tables were created successfully, False otherwise
    """
    logger.info("Setting up database tables")
    
    # Ensure database exists
    if not ensure_database_exists():
        return False
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        # Create tables
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLES_SQL)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database tables created successfully")
        return True
    
    except psycopg2.Error as e:
        logger.error(f"Error creating tables: {e}")
        if conn:
            conn.close()
        return False

def load_processed_files() -> Dict[str, datetime.datetime]:
    """
    Load index of processed files.
    
    Returns:
        Dict[str, datetime.datetime]: Dictionary of processed files with timestamps
    """
    if not os.path.exists(PROCESSED_FILES_INDEX):
        return {}
    
    try:
        with open(PROCESSED_FILES_INDEX, "r") as f:
            data = json.load(f)
            return {k: datetime.datetime.fromisoformat(v) for k, v in data.items()}
    except json.JSONDecodeError:
        logger.error("Error decoding processed_files_index.json")
        return {}

def save_processed_files(processed_files: Dict[str, datetime.datetime]):
    """
    Save index of processed files.
    
    Args:
        processed_files: Dictionary of processed files with timestamps
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(PROCESSED_FILES_INDEX), exist_ok=True)
    
    # Convert datetime objects to ISO format strings
    data = {k: v.isoformat() for k, v in processed_files.items()}
    
    with open(PROCESSED_FILES_INDEX, "w") as f:
        json.dump(data, f, indent=4)

def get_new_files(processed_files: Dict[str, datetime.datetime]) -> List[str]:
    """
    Get list of new files to process.
    
    Args:
        processed_files: Dictionary of already processed files
    
    Returns:
        List[str]: List of new file paths to process
    """
    # Create directory if it doesn't exist
    os.makedirs(NEWS_DATA_DIR, exist_ok=True)
    
    # Get all JSON files in the directory
    json_files = [f for f in os.listdir(NEWS_DATA_DIR) if f.endswith('.json')]
    
    # Filter out already processed files
    new_files = [f for f in json_files if f not in processed_files]
    
    return [os.path.join(NEWS_DATA_DIR, f) for f in new_files]

def process_news_article(conn, article_data: Dict[str, Any], source_file: str) -> Optional[int]:
    """
    Process a single news article and insert it into the database.
    
    Args:
        conn: Database connection
        article_data: Article data dictionary
        source_file: Source file name
    
    Returns:
        Optional[int]: Article ID if successfully inserted, None otherwise
    """
    # Check if article already exists
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM news_articles WHERE url = %s", (article_data["url"],))
    existing = cursor.fetchone()
    if existing:
        logger.info(f"Article already exists: {article_data['url']}")
        cursor.close()
        return existing[0]
    
    try:
        # Parse published date
        published_date = datetime.datetime.fromisoformat(article_data["published_date"].replace('Z', '+00:00'))
        scraped_at = datetime.datetime.fromisoformat(article_data["scraped_at"])
        
        # Get tags as array
        tags = article_data.get("tags", [])
        
        # Insert article
        cursor.execute("""
            INSERT INTO news_articles (
                url, title, author, published_date, description, body,
                source, scraped_at, file_source, processed_at, tags
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            article_data["url"],
            article_data.get("title"),
            article_data.get("author"),
            published_date,
            article_data.get("description"),
            article_data.get("body"),
            article_data.get("source"),
            scraped_at,
            source_file,
            datetime.datetime.now(),
            tags
        ))
        
        article_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Added article: {article_data['url']}")
        cursor.close()
        return article_id
    
    except psycopg2.Error as e:
        logger.error(f"Error inserting article {article_data['url']}: {e}")
        conn.rollback()
        cursor.close()
        return None

def process_file(file_path: str) -> bool:
    """
    Process a single news JSON file.
    
    Args:
        file_path: Path to the JSON file
    
    Returns:
        bool: True if file was processed successfully, False otherwise
    """ 
    if 'url_index' in file_path:
        logger.info(f"Skipping URL index file: {file_path}")
        return True
    
    logger.info(f"Processing file: {file_path}")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Get database connection
        conn = get_db_connection()
        if not conn:
            return False
        
        try:
            # Process each article
            for article_data in data:
                process_news_article(conn, article_data, os.path.basename(file_path))
            
            conn.close()
            return True
        
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            conn.close()
            return False
    
    except Exception as e:
        logger.error(f"Error opening file {file_path}: {e}")
        return False

def process_all_new_files():
    """
    Process all new news JSON files.
    
    Returns:
        bool: True if all files were processed successfully, False otherwise
    """
    # Load processed files index
    processed_files = load_processed_files()
    
    # Get new files
    new_files = get_new_files(processed_files)
    
    if not new_files:
        logger.info("No new files to process")
        return True
    
    logger.info(f"Found {len(new_files)} new files to process")
    
    success = True
    
    # Process each file
    for file_path in new_files:
        file_success = process_file(file_path)
        if file_success:
            logger.info(f"Successfully processed {file_path}")
            processed_files[os.path.basename(file_path)] = datetime.datetime.now()
            save_processed_files(processed_files)
        else:
            logger.error(f"Failed to process {file_path}")
            success = False
    
    return success

def main():
    """
    Main entry point.
    """
    # Set up database
    if not setup_database():
        logger.error("Failed to set up database")
        return 1
    
    # Process all new files
    if not process_all_new_files():
        logger.error("Failed to process all new files")
        return 1
    
    logger.info("News data processing completed successfully")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 