"""
Scrapy pipelines for processing Reddit data.
"""

import json
import os
import datetime
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.reddit_config import OUTPUT_CONFIG

class RedditJsonPipeline:
    """
    Pipeline for processing Reddit data and saving to JSON files.
    """
    
    def __init__(self):
        # Get output folder from config
        self.output_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/reddit')
        ).replace('../', '')
        
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Set the path for the cumulative data file
        self.cumulative_file = os.path.join(self.output_folder, 'reddit_data_cumulative.json')
        
        
        # Also keep track of posts in the current scraping session
        self.current_session_posts = []
    
    
    def process_item(self, item, spider):
        """
        Process each scraped item.
        
        Args:
            item: The scraped Reddit post item
            spider: The Spider instance
            
        Returns:
            The processed item
        """
        
        # Add the item to both the current session posts and the cumulative posts
        post_dict = dict(item)
        for key, value in post_dict.items():
            if isinstance(value, list):
                post_dict[key] = [str(item) for item in value]
        self.current_session_posts.append(post_dict)
        
        return item
    
    def close_spider(self, spider):
        """
        Called when the spider is closed. Writes all data to files.
        
        Args:
            spider: The Spider instance
        """
        # Save the current session posts to a timestamped file
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        session_filename = f'reddit_{timestamp}.json'
        session_filepath = os.path.join(self.output_folder, session_filename)
        
        # Write current session data to file
        with open(session_filepath, 'w', encoding='utf-8') as f:
            json.dump(self.current_session_posts, f, indent=4, ensure_ascii=False)
        
        spider.logger.info(f"Saved {len(self.current_session_posts)} posts from current session to {session_filepath}")


class RedditCsvPipeline:
    """
    Pipeline for processing Reddit data and saving to CSV files.
    Each post will be stored as a single row in the CSV file, with comments/replies 
    stored as a JSON string in a single column.
    """
    
    def __init__(self):
        # Get output folder from config
        self.output_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/reddit')
        ).replace('../', '')
        
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Initialize list to store posts
        self.posts = []
        
        # CSV headers - all the fields we want to extract from the posts
        self.csv_headers = [
            'id', 'title', 'url', 'author', 'score', 'num_comments', 
            'subreddit', 'created', 'created_utc', 'content_type', 
            'content', 'body_text', 'comments_json'
        ]
    
    def process_item(self, item, spider):
        """
        Process each scraped item.
        
        Args:
            item: The scraped Reddit post item
            spider: The Spider instance
            
        Returns:
            The processed item
        """
        # Convert the item to a dictionary
        post_dict = dict(item)
        
        # Extract all fields for the CSV row
        csv_row = {}
        for header in self.csv_headers:
            if header == 'comments_json':
                # Convert comments array to JSON string
                comments = post_dict.get('comments', [])
                csv_row[header] = json.dumps(comments, ensure_ascii=False)
            elif header in post_dict:
                # Copy other fields directly
                csv_row[header] = post_dict[header]
            else:
                # Set missing fields to empty string
                csv_row[header] = ""
        
        # Add to our posts list
        self.posts.append(csv_row)
        
        # Return the item for potential further processing
        return item
    
    def close_spider(self, spider):
        """
        Called when the spider is closed. Writes all data to CSV file.
        
        Args:
            spider: The Spider instance
        """
        import csv
        
        # Generate timestamp for filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f'reddit_{timestamp}.csv'
        csv_filepath = os.path.join(self.output_folder, csv_filename)
        
        # Write to CSV file
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.csv_headers)
            writer.writeheader()
            for post in self.posts:
                writer.writerow(post)
        
        spider.logger.info(f"Saved {len(self.posts)} posts to CSV file: {csv_filepath}") 