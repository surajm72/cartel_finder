"""
Configuration settings for Reddit data collection.
"""

# List of subreddits to scrape
#r/news
# r/legaladvice
# r/smallbusiness
# r/productfails
SUBREDDITS = [
    'productfails',
    'news',
    'legaladvice',
    'smallbusiness'
]

# Scraping settings
SCRAPING_CONFIG = {
    # Delay between requests (in seconds)
    'request_delay': 2.0,  # Increased for more reliability
    
    # Number of posts to scrape per subreddit
    'posts_per_subreddit': 25,  # Reduced for initial testing
    
    # Number of comments to scrape per post
    'comments_per_post': 25,
    
    # Sort method for posts
    'sort_method': 'new',  # Options: 'hot', 'new', 'top', 'rising'
    
    # Time filter for top posts
    'time_filter': 'month',  # Options: 'all', 'day', 'week', 'month', 'year'
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Retry delay (in seconds)
    'retry_delay': 5.0,
    
    # Whether to use proxies (can be enabled later)
    'use_proxies': False,
    
    # Whether to scrape media (images, videos)
    'scrape_media': False,
    
    # Whether to scrape NSFW content
    'include_nsfw': False,
}

# Output settings
OUTPUT_CONFIG = {
    # Format for raw data files
    'output_format': 'json',  # Options: 'json', 'csv'
    
    
    # Folder to save raw data
    'output_folder': 'raw_data/reddit/',
    
    # File naming pattern
    'file_pattern': '{subreddit}_{timestamp}.{format}',
}

# Proxy configuration (for future use)
PROXY_CONFIG = {
    'proxies': [],
    'proxy_rotation_strategy': 'random',  # Options: 'random', 'sequential'
    'proxy_auth': {
        'username': '',
        'password': ''
    }
} 