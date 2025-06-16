"""
Runner script for the News scrapers.
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

# Import configs
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from config.news_config import NEWS_SOURCES

def run_spider():
    """Run all enabled news spiders from config."""
    # Set up settings
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'data_collection.news.settings'
    settings = get_project_settings()
    
    # Create process
    process = CrawlerProcess(settings)
    
    # Add all enabled spiders from config
    for source, config in NEWS_SOURCES.items():
        if config.get('enabled', False):
            # Import the spider class
            spider_module = __import__(f'data_collection.news.spiders.{source}_spider', 
                                     fromlist=[f'{source.capitalize()}Spider'])
            spider_class = getattr(spider_module, f'{source.capitalize()}Spider')
            
            # Add spider to process
            process.crawl(spider_class)
    
    # Start all spiders
    process.start()

if __name__ == "__main__":
    run_spider() 