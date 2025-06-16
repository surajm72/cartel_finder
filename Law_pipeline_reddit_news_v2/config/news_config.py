"""
Configuration settings for news data collection.
"""

# Reuters scraping configuration
REUTERS_CONFIG = {
    # Number of articles to scrape per page
    'articles_per_page': 10,
    
    # Delay between requests (in seconds)
    'request_delay': 5.0,
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Whether to scrape the full article body
    'scrape_article_body': True,
    
    # Default date range for scraping (0 = today, 1 = yesterday, etc.)
    'days_back': 1,
    
    # Maximum number of articles to scrape (None = no limit)
    'max_articles': 5,
    
    # Custom date range (use YYYY-MM-DD format)
    # If provided, will override days_back
    'custom_date_range': {
        'enabled': False,
        'start_date': '2025-05-01',
        'end_date': '2025-05-02'
    },
    
    # Filter options for sections/categories
    'section_filters': {
        'enabled': False,
        'include': ['business', 'world', 'markets', 'legal'],  # Empty list means include all
        'exclude': []  # Sections to exclude
    },
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

# TechCrunch scraping configuration
TECHCRUNCH_CONFIG = {
    # Number of articles to scrape per page
    'articles_per_page': 10,
    
    # Delay between requests (in seconds)
    'request_delay': 3.0,
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Whether to scrape the full article body
    'scrape_article_body': True,
    
    # Default date range for scraping (0 = today, 1 = yesterday, etc.)
    'days_back': 1,
    
    # Maximum number of articles to scrape (None = no limit)
    'max_articles': 5,
    
    # Custom date range (use YYYY-MM-DD format)
    # If provided, will override days_back
    'custom_date_range': {
        'enabled': False,
        'start_date': '2025-05-01',
        'end_date': '2025-05-03'
    },
    
    # Filter options for sections/categories
    'section_filters': {
        'enabled': False,
        'include': ['startups', 'ai', 'security', 'apps'],  # Empty list means include all
        'exclude': []  # Sections to exclude
    },
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
}

# Patch.com scraping configuration
PATCH_CONFIG = {
    # Delay between requests (in seconds)
    'request_delay': 3.0,
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Whether to scrape the full article body
    'scrape_article_body': True,
    
    # Maximum number of articles to scrape (None = no limit)
    'max_articles': 5,
    
    # Maximum number of pages to scrape per URL
    'max_pages': 3,
    
    # Time filter (in hours, None = no filter)
    'time_filter': None,  # Only articles from last 24 hours
    
    # Filter options for sections/categories
    'section_filters': {
        'enabled': False,
        'include': ['News', 'Crime & Safety', 'Politics & Government'],  # Empty list means include all
        'exclude': []  # Sections to exclude
    },
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
}

# Axios scraping configuration
AXIOS_CONFIG = {
    # Delay between requests (in seconds)
    'request_delay': 3.0,
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Whether to scrape the full article body
    'scrape_article_body': True,
    
    # Maximum number of articles to scrape (None = no limit)
    'max_articles': 5,
    
    # Time filter (in hours, None = no filter)
    'time_filter': 24,  # Only articles from last 24 hours
    
    # Filter options for sections/categories
    'section_filters': {
        'enabled': False,
        'include': ['News', 'Politics', 'Business'],  # Empty list means include all
        'exclude': []  # Sections to exclude
    },
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    
    # API endpoints
    'api_endpoints': {
        'content_list': 'https://www.axios.com/api/v1/mixed-content',
        'article_detail': 'https://www.axios.com/api/axios-web/dto/card/{article_id}'
    },
    
    # API parameters
    'api_params': {
        'status': 'published',
        'order_by': 1,
        'page_size': 100
    }
}

# AP News scraping configuration
APNEWS_CONFIG = {
    # Delay between requests (in seconds)
    'request_delay': 3.0,
    
    # Maximum retries for failed requests
    'max_retries': 3,
    
    # Whether to scrape the full article body
    'scrape_article_body': True,
    
    # Maximum number of articles to scrape (None = no limit)
    'max_articles': 5,
    
    # Maximum number of pages to scrape
    'max_pages': 5,
    
    # Time filter (in hours, None = no filter)
    'time_filter': 24,  # Only articles from last 24 hours
    
    # Filter options for sections/categories
    'section_filters': {
        'enabled': False,
        'include': ['Politics', 'Business', 'Technology', 'Science'],  # Empty list means include all
        'exclude': []  # Sections to exclude
    },
    
    # User agent to use for requests
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    
    # Base URL for top news
    'base_url': 'https://apnews.com/hub/ap-top-news'
}

# News sources configuration
NEWS_SOURCES = {
    'reuters': {
        'enabled': True,
        'base_url': 'https://www.reuters.com',
        'sitemap_url': 'https://www.reuters.com/sitemap/{year}-{month:02d}/{day:02d}/{page}/',
        'config': REUTERS_CONFIG
    },
    'techcrunch': {
        'enabled': True,
        'base_url': 'https://techcrunch.com',
        'sitemap_url': 'https://techcrunch.com/{year}/{month:02d}/{day:02d}/',
        'config': TECHCRUNCH_CONFIG
    },
    'patch': {
        'enabled': True,
        'base_url': 'https://patch.com',
        'source_urls': [
            'https://patch.com/us/across-america/topics/patch-exclusives'
        ],
        'config': PATCH_CONFIG
    },
    'axios': {
        'enabled': True,
        'base_url': 'https://www.axios.com',
        'config': AXIOS_CONFIG
    },
    'apnews': {
        'enabled': True,
        'base_url': 'https://apnews.com',
        'config': APNEWS_CONFIG
    },
    # Add more news sources here as they're implemented
}

# Output settings
OUTPUT_CONFIG = {
    # Format for raw data files
    'output_format': 'json',
    
    # Folder to save raw data
    'output_folder': 'raw_data/news/',
    
    # File naming pattern
    'file_pattern': '{source}_{timestamp}.{format}',
} 