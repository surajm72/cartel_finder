"""
Scrapy pipelines for processing News data.
"""

import json
import csv
import os
import datetime
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.news_config import OUTPUT_CONFIG

class NewsJsonPipeline:
    """
    Pipeline for processing News data and saving to JSON files.
    """
    
    def __init__(self):
        # Get output folder from config
        self.output_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/news')
        ).replace('../', '')
        
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Initialize items dictionary for different sources
        self.items_by_source = {}
        
        # For tracking progress
        self.item_count = 0
    
    def process_item(self, item, spider):
        """
        Process each scraped item.
        
        Args:
            item: The scraped article item
            spider: The Spider instance
            
        Returns:
            The processed item
        """
        # Convert item to dictionary and ensure it has a source
        item_dict = dict(item)
        source = item_dict.get('source', 'unknown')
        
        # Initialize source list if needed
        if source not in self.items_by_source:
            self.items_by_source[source] = []
        
        # Add the item to the source-specific list
        self.items_by_source[source].append(item_dict)
        
        # Update item count and log progress
        self.item_count += 1
        
        # Log every 5 items
        if self.item_count % 5 == 0:
            max_articles = getattr(spider, 'max_articles', None)
            if max_articles:
                spider.logger.info(f"Collected {self.item_count}/{max_articles} articles")
            else:
                spider.logger.info(f"Collected {self.item_count} articles")
                
            # # Auto-save data periodically
            # if self.item_count % 20 == 0:
            #     self._save_data(spider, is_final=False)
        
        return item
    
    def _save_data(self, spider, is_final=True):
        """
        Save collected data to files.
        
        Args:
            spider: The Spider instance
            is_final: Whether this is the final save on spider close
        """
        # Get the current timestamp for filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # For each source, write a separate file
        for source, items in self.items_by_source.items():
            if not items:
                continue
                
            # Get the date for filename (use the first item's date if available)
            date = items[0].get('date', datetime.date.today().strftime('%Y-%m-%d'))
            
            # Format the filename
            filename = OUTPUT_CONFIG.get('file_pattern', '{source}_{timestamp}.{format}').format(
                source=source,
                timestamp=timestamp,
                format='json'
            )
            
            # Add indicator if this isn't the final save
            if not is_final:
                filename = f"partial_{filename}"
            
            # Write data to file
            filepath = os.path.join(self.output_folder, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=4, ensure_ascii=False)
            
            spider.logger.info(f"Saved {len(items)} articles from {source} to {filepath}")
    
    def close_spider(self, spider):
        """
        Called when the spider is closed. Writes all data to files.
        
        Args:
            spider: The Spider instance
        """
        self._save_data(spider, is_final=True)


class NewsCsvPipeline:
    """
    Pipeline for processing News data and saving to CSV files.
    """
    
    def __init__(self):
        # Get output folder from config
        self.output_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/news')
        ).replace('../', '')
        
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Initialize items dictionary for different sources
        self.items_by_source = {}
    
    def process_item(self, item, spider):
        """
        Process each scraped item.
        
        Args:
            item: The scraped article item
            spider: The Spider instance
            
        Returns:
            The processed item
        """
        # Convert item to dictionary and ensure it has a source
        item_dict = dict(item)
        source = item_dict.get('source', 'unknown')
        
        # Initialize source list if needed
        if source not in self.items_by_source:
            self.items_by_source[source] = []
        
        # Add the item to the source-specific list
        self.items_by_source[source].append(item_dict)
        
        return item
    
    def _save_data(self, spider, is_final=True):
        """
        Save collected data to CSV files.
        
        Args:
            spider: The Spider instance
            is_final: Whether this is the final save on spider close
        """
        # Get the current timestamp for filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # For each source, write a separate file
        for source, items in self.items_by_source.items():
            if not items:
                continue
                
            # Get the date for filename (use the first item's date if available)
            date = items[0].get('date', datetime.date.today().strftime('%Y-%m-%d'))
            
            # Format the filename
            filename = OUTPUT_CONFIG.get('file_pattern', '{source}_{timestamp}.{format}').format(
                source=source,
                timestamp=timestamp,
                format='csv'
            )
            
            # Add indicator if this isn't the final save
            if not is_final:
                filename = f"partial_{filename}"
            
            # Write data to CSV file
            filepath = os.path.join(self.output_folder, filename)
            
            # Get all possible fieldnames from all items
            fieldnames = set()
            for item in items:
                fieldnames.update(item.keys())
            
            # Sort fieldnames for consistency, but put common important fields first
            priority_fields = ['title', 'url', 'date', 'published_date', 'source', 'author', 'description', 'body']
            sorted_fields = [f for f in priority_fields if f in fieldnames]
            sorted_fields.extend(sorted([f for f in fieldnames if f not in priority_fields]))
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=sorted_fields)
                writer.writeheader()
                for item in items:
                    writer.writerow(item)
            
            spider.logger.info(f"Saved {len(items)} articles from {source} to {filepath}")
    
    def close_spider(self, spider):
        """
        Called when the spider is closed. Writes all data to CSV files.
        
        Args:
            spider: The Spider instance
        """
        self._save_data(spider, is_final=True) 