"""
Runner script for the Reddit scraper.
"""

import os
import sys
import asyncio
from twisted.internet import asyncioreactor

# Add the project root to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

# Try to install asyncio reactor - works on all platforms
try:
    asyncioreactor.install()
except Exception:
    pass

# Import the spider
from data_collection.reddit.spiders.reddit_spider import RedditSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def run_spider():
    """Run the Reddit spider."""
    # Set up settings
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'data_collection.reddit.settings'
    settings = get_project_settings()
    
    # Create and run spider
    process = CrawlerProcess(settings)
    process.crawl(RedditSpider)
    process.start()

if __name__ == "__main__":
    run_spider() 