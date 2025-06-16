#!/usr/bin/env python
"""
Simplified Reddit data processor.
This script handles both database setup and data processing in one file.
"""
import os
import sys
import json
import logging
import datetime
import ast
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from config.database_config import DB_CONFIG, get_db_connection, ensure_database_exists

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/reddit_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("reddit_processor")

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)

# SQL for creating tables
CREATE_TABLES_SQL = """
-- Reddit posts table
CREATE TABLE IF NOT EXISTS reddit_posts (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    url VARCHAR,
    author VARCHAR,
    score VARCHAR,
    num_comments VARCHAR,
    subreddit VARCHAR,
    created VARCHAR,
    created_utc FLOAT,
    external_url VARCHAR,
    content_type VARCHAR,
    content VARCHAR,
    body_text TEXT,
    file_source VARCHAR,
    processed_at TIMESTAMP
);

-- Reddit comments table
CREATE TABLE IF NOT EXISTS reddit_comments (
    id VARCHAR PRIMARY KEY,
    post_id VARCHAR REFERENCES reddit_posts(id) ON DELETE CASCADE,
    parent_comment_id VARCHAR REFERENCES reddit_comments(id) ON DELETE CASCADE,
    author VARCHAR,
    created VARCHAR,
    created_utc FLOAT,
    body_text TEXT,
    score_dislikes VARCHAR,
    score_unvoted VARCHAR,
    score_likes VARCHAR,
    processed_at TIMESTAMP
);
"""

# Path to Reddit raw data files
REDDIT_DATA_DIR = "raw_data/reddit"
# Path to processed files index - moved to data_pipeline folder
PROCESSED_FILES_INDEX = os.path.join("data_pipeline", "processed_reddit_files_index.json")

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
            # Handle both dictionary and list format
            if isinstance(data, dict):
                return {k: datetime.datetime.fromisoformat(v) for k, v in data.items()}
            else:
                logger.warning("Processed files index is not in expected format, creating new index")
                return {}
    except json.JSONDecodeError:
        logger.error("Error decoding processed_reddit_files_index.json")
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
    os.makedirs(REDDIT_DATA_DIR, exist_ok=True)
    
    # Get all JSON files in the directory
    json_files = [f for f in os.listdir(REDDIT_DATA_DIR) if f.endswith('.json') and f != "processed_reddit_files_index.json"]
    
    # Filter out already processed files
    new_files = [f for f in json_files if f not in processed_files]
    
    return [os.path.join(REDDIT_DATA_DIR, f) for f in new_files]


def parse_comment_str(comment_str: str) -> Dict[str, Any]:
    """
    Parse a comment string into a dictionary.
    
    Args:
        comment_str: Raw comment string
    
    Returns:
        Dict containing parsed comment data
    """
    if not comment_str:
        return {}
    
    try:
        # Using ast.literal_eval to safely evaluate the string as a Python literal
        return ast.literal_eval(comment_str)
    except (SyntaxError, ValueError) as e:
        logger.error(f"Error parsing comment string: {e}")
        # Try alternative parsing methods if literal_eval fails
        return fallback_parse(comment_str)


def fallback_parse(comment_str: str) -> Dict[str, Any]:
    """
    Fallback method to parse comment strings if ast.literal_eval fails.
    
    Args:
        comment_str: Raw comment string
    
    Returns:
        Dict containing parsed comment data
    """
    # Try to clean up and fix common issues in the comment string
    try:
        # Fix nested single quotes issue
        fixed_str = comment_str.replace("\\'", "'").replace("\\\"", "\"")
        # Try to parse with json by replacing single quotes with double quotes
        json_compatible = re.sub(r"'([^']*)':", r'"\1":', fixed_str)
        json_compatible = re.sub(r": '([^']*)'", r': "\1"', json_compatible)
        return json.loads(json_compatible)
    except (json.JSONDecodeError, Exception) as e:
        logger.error(f"Fallback parsing failed: {e}")
        return {}


def extract_comments(post_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract comments from post data.
    
    Args:
        post_data: Post data dictionary
    
    Returns:
        List of parsed comment dictionaries
    """
    comments = []
    
    # Skip if no comments
    if "comments" not in post_data or not post_data["comments"]:
        return comments
    
    for comment_str in post_data["comments"]:
        comment_dict = parse_comment_str(comment_str)
        if comment_dict:
            comments.append(comment_dict)
    
    return comments


def process_reddit_post(conn, post_data: Dict[str, Any], source_file: str) -> Optional[str]:
    """
    Process a single Reddit post and insert it into the database.
    
    Args:
        conn: Database connection
        post_data: Post data dictionary
        source_file: Source file name
    
    Returns:
        Optional[str]: Post ID if successfully inserted, None otherwise
    """
    # Check if post already exists
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM reddit_posts WHERE id = %s", (post_data["id"],))
    if cursor.fetchone():
        logger.info(f"Post already exists: {post_data['id']}")
        cursor.close()
        return post_data["id"]
    
    # Insert post
    try:
        cursor.execute("""
            INSERT INTO reddit_posts (
                id, title, url, author, score, num_comments, subreddit, created,
                created_utc, external_url, content_type, content, body_text,
                file_source, processed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            post_data["id"],
            post_data.get("title"),
            post_data.get("url"),
            post_data.get("author"),
            post_data.get("score"),
            post_data.get("num_comments"),
            post_data.get("subreddit"),
            post_data.get("created"),
            post_data.get("created_utc"),
            post_data.get("external_url"),
            post_data.get("content_type"),
            post_data.get("content"),
            post_data.get("body_text"),
            source_file,
            datetime.datetime.now()
        ))
        conn.commit()
        logger.info(f"Added post: {post_data['id']}")
        cursor.close()
        return post_data["id"]
    
    except psycopg2.Error as e:
        logger.error(f"Error inserting post {post_data['id']}: {e}")
        conn.rollback()
        cursor.close()
        return None


def process_reddit_comment(conn, comment_dict: Dict[str, Any], post_id: str, parent_comment_id: Optional[str] = None) -> Optional[str]:
    """
    Process a single Reddit comment and insert it into the database.
    
    Args:
        conn: Database connection
        comment_dict: Comment data dictionary
        post_id: Parent post ID
        parent_comment_id: Parent comment ID for nested comments
    
    Returns:
        Optional[str]: Comment ID if successfully inserted, None otherwise
    """
    # Skip if no comment ID
    if "id" not in comment_dict:
        logger.warning(f"Comment missing ID for post {post_id}")
        return None
    
    # Check if comment already exists
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM reddit_comments WHERE id = %s", (comment_dict["id"],))
    if cursor.fetchone():
        logger.info(f"Comment already exists: {comment_dict['id']}")
        cursor.close()
        return comment_dict["id"]
    
    # Insert comment
    try:
        cursor.execute("""
            INSERT INTO reddit_comments (
                id, post_id, parent_comment_id, author, created, created_utc, body_text, score_dislikes,
                score_unvoted, score_likes, processed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            comment_dict["id"],
            post_id,
            parent_comment_id,
            comment_dict.get("author"),
            comment_dict.get("created"),
            comment_dict.get("created_utc"),
            comment_dict.get("body_text"),
            comment_dict.get("score_dislikes"),
            comment_dict.get("score_unvoted"),
            comment_dict.get("score_likes"),
            datetime.datetime.now()
        ))
        conn.commit()
        logger.info(f"Added comment: {comment_dict['id']}")
        
        # Process any replies to this comment
        if "replies" in comment_dict and comment_dict["replies"]:
            for reply_dict in comment_dict["replies"]:
                process_reddit_comment(conn, reply_dict, post_id, comment_dict["id"])
        
        cursor.close()
        return comment_dict["id"]
    
    except psycopg2.Error as e:
        logger.error(f"Error inserting comment {comment_dict['id']}: {e}")
        conn.rollback()
        cursor.close()
        return None


def process_file(file_path: str) -> bool:
    """
    Process a single Reddit JSON file.
    
    Args:
        file_path: Path to the JSON file
    
    Returns:
        bool: True if file was processed successfully, False otherwise
    """
    logger.info(f"Processing file: {file_path}")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Get database connection
        conn = get_db_connection()
        if not conn:
            return False
        
        try:
            # Process each post
            for post_data in data:
                # Process post
                post_id = process_reddit_post(conn, post_data, os.path.basename(file_path))
                if post_id:
                    # Process comments
                    comments = extract_comments(post_data)
                    for comment_dict in comments:
                        # Process top-level comments (no parent comment)
                        process_reddit_comment(conn, comment_dict, post_id, None)
            
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
    Process all new Reddit JSON files.
    
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
    
    logger.info("Reddit data processing completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main()) 