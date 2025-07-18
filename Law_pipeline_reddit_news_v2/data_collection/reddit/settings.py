"""
Scrapy settings for Reddit scraper.
"""

import os
import sys
import datetime
import logging

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.reddit_config import SCRAPING_CONFIG, OUTPUT_CONFIG

BOT_NAME = 'reddit_scraper'

SPIDER_MODULES = ['data_collection.reddit.spiders']
NEWSPIDER_MODULE = 'data_collection.reddit.spiders'

# Obey robots.txt rules - Disabled for Reddit as it blocks scrapers
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 1  # Start with 1 to be safe, can increase later

# Configure delay between requests to avoid being blocked
DOWNLOAD_DELAY = SCRAPING_CONFIG.get('request_delay', 2.0)
RANDOMIZE_DOWNLOAD_DELAY = True

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Set the default User-Agent
USER_AGENT = SCRAPING_CONFIG.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

# Respect the retry settings from config
RETRY_TIMES = SCRAPING_CONFIG.get('max_retries', 3)
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# Enable and configure HTTP caching (disabled by default)
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400  # 24 hours
HTTPCACHE_DIR = 'httpcache'

# Configure item pipelines
ITEM_PIPELINES = {
    'data_collection.reddit.pipelines.RedditJsonPipeline': 300,
    'data_collection.reddit.pipelines.RedditCsvPipeline': 400,
}

# Set the log level - Only critical logs from Scrapy
LOG_LEVEL = 'INFO'
LOG_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    f'logs/reddit_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)

# Create logs directory if it doesn't exist
if not os.path.exists(os.path.dirname(LOG_FILE)):
    os.makedirs(os.path.dirname(LOG_FILE))

# Define a filter to block Scrapy logs but allow spider logs
class IgnoreScrapyLogs(logging.Filter):
    def filter(self, record):
        # Allow logs from 'reddit' and block logs from 'scrapy'
        return 'scrapy.' not in record.name

# Apply the filter to the root logger
logging.getLogger().addFilter(IgnoreScrapyLogs()) 