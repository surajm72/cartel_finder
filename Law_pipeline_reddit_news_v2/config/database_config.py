"""
Configuration settings for database connections.
"""
import os
import configparser
import logging
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Set up logging
logger = logging.getLogger(__name__)

# Find the root directory
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
config_file = os.path.join(root_dir, 'config.ini')

# Create a ConfigParser instance
config = configparser.ConfigParser()

# Read the configuration file
if os.path.exists(config_file):
    config.read(config_file)
else:
    # If config file doesn't exist, use default values
    config['database'] = {
        'host': 'localhost',
        'port': '5432',
        'username': 'postgres',
        'password': 'password',
        'database': 'legal_data'
    }

# Database connection parameters
DB_CONFIG = {
    # PostgreSQL connection parameters read from config.ini
    'postgres': {
        'host': config.get('database', 'host', fallback='localhost'),
        'port': config.get('database', 'port', fallback='5432'),
        'username': config.get('database', 'username', fallback='postgres'),
        'password': config.get('database', 'password', fallback='password'),
        'database': config.get('database', 'database', fallback='reddit_data')
    },
    
    # Add other database configurations if needed in the future
    # For example, MySQL or SQLite
    
    # Default database type to use
    'default_db': 'postgres'
}

def get_db_connection(db_type=None):
    """
    Get a connection to the database.
    
    Args:
        db_type: Type of database to connect to, defaults to the default_db in config
        
    Returns:
        Connection object or None if connection fails
    """
    if db_type is None:
        db_type = DB_CONFIG['default_db']
        
    if db_type == 'postgres':
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG['postgres']['host'],
                port=DB_CONFIG['postgres']['port'],
                user=DB_CONFIG['postgres']['username'],
                password=DB_CONFIG['postgres']['password'],
                database=DB_CONFIG['postgres']['database']
            )
            return conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            return None
    else:
        logger.error(f"Unsupported database type: {db_type}")
        return None

def ensure_database_exists(db_name=None):
    """
    Ensure that the database exists, creating it if necessary.
    
    Args:
        db_name: Name of the database to check/create, defaults to the one in config
        
    Returns:
        bool: True if database exists or was created, False otherwise
    """
    if db_name is None:
        db_name = DB_CONFIG['postgres']['database']
    
    logger.info(f"Checking if database '{db_name}' exists")
    
    try:
        # Try to connect to the database
        conn = psycopg2.connect(
            host=DB_CONFIG['postgres']['host'],
            port=DB_CONFIG['postgres']['port'],
            user=DB_CONFIG['postgres']['username'],
            password=DB_CONFIG['postgres']['password'],
            database=db_name
        )
        conn.close()
        logger.info(f"Database '{db_name}' already exists")
        return True
    except psycopg2.Error:
        logger.info(f"Database '{db_name}' does not exist. Creating it...")
        
        try:
            # Connect to PostgreSQL server
            conn = psycopg2.connect(
                host=DB_CONFIG['postgres']['host'],
                port=DB_CONFIG['postgres']['port'],
                user=DB_CONFIG['postgres']['username'],
                password=DB_CONFIG['postgres']['password']
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Create database
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {db_name}")
            cursor.close()
            conn.close()
            
            logger.info(f"Created database '{db_name}'")
            return True
        
        except psycopg2.Error as e:
            logger.error(f"Error creating database: {e}")
            return False

# For testing/debugging
if __name__ == "__main__":
    print("Database Configuration:")
    for key, value in DB_CONFIG['postgres'].items():
        # Hide password in logs
        if key == 'password':
            print(f"  {key}: {'*' * len(value)}")
        else:
            print(f"  {key}: {value}")
    
    # Test database connection
    conn = get_db_connection()
    if conn:
        print("Successfully connected to database")
        conn.close() 