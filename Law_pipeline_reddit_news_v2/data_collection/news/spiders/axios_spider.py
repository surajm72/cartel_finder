import scrapy
import json
import time
import datetime
import os
import sys
import logging
from urllib.parse import urljoin
import requests
from scrapy.exceptions import CloseSpider
from bs4 import BeautifulSoup

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from config.news_config import NEWS_SOURCES, OUTPUT_CONFIG

class AxiosSpider(scrapy.Spider):
    name = "axios"
    allowed_domains = ["axios.com"]
    
    def __init__(self, max_articles=None, max_pages=None, 
                 scrape_body=None, time_filter=None, 
                 source_urls=None, *args, **kwargs):
        super(AxiosSpider, self).__init__(*args, **kwargs)
        
        # Get Axios config
        self.source_config = NEWS_SOURCES['axios']
        self.config = self.source_config['config']
        
        # Override config with command line arguments if provided
        if max_articles is not None:
            self.max_articles = int(max_articles)
        else:
            self.max_articles = self.config.get('max_articles')
            
        if max_pages is not None:
            self.max_pages = int(max_pages)
        else:
            self.max_pages = self.config.get('max_pages', 1)
            
        if scrape_body is not None:
            self.config['scrape_article_body'] = (scrape_body.lower() == 'true')
            
        if time_filter is not None:
            self.config['time_filter'] = int(time_filter)
        
        # Counter for articles scraped
        self.article_count = 0
        
        # Counter for articles requested
        self.articles_requested = 0
        
        # Set request delay
        self.request_delay = self.config['request_delay']
        
        # Set up user agent
        self.user_agent = self.config['user_agent']
        
        # Initialize processed URLs set
        self.processed_urls = set()
        
        # URL tracking file path
        self.url_index_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/news/'),
            'axios_url_index.json'
        )
        
        # Load previously processed URLs if file exists
        self.load_processed_urls()
        
        # Create output directory if it doesn't exist
        self.output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/news/')
        )
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Print configuration information
        self._print_config_info()
    
    def _print_config_info(self):
        """Print detailed configuration information."""
        self.logger.info("========= AXIOS SPIDER CONFIGURATION =========")
        self.logger.info(f"Maximum Articles: {self.max_articles if self.max_articles else 'No limit'}")
        self.logger.info(f"Maximum Pages: {self.max_pages}")
        self.logger.info(f"Scrape Article Body: {self.config['scrape_article_body']}")
        self.logger.info(f"Request Delay: {self.config['request_delay']} seconds")
        self.logger.info(f"Time Filter: {self.config['time_filter']} hours")
        
        if self.config['section_filters']['enabled']:
            include_sections = self.config['section_filters']['include'] or "All sections"
            exclude_sections = self.config['section_filters']['exclude'] or "None"
            self.logger.info(f"Section Filters: Include={include_sections}, Exclude={exclude_sections}")
        else:
            self.logger.info("Section Filters: Disabled (including all sections)")
        
        self.logger.info("==============================================")
    
    def load_processed_urls(self):
        """Load previously processed URLs from the index file."""
        if os.path.exists(self.url_index_file):
            try:
                with open(self.url_index_file, 'r', encoding='utf-8') as f:
                    url_data = json.load(f)
                    self.processed_urls = set(url_data.get('processed_urls', []))
                self.logger.info(f"Loaded {len(self.processed_urls)} previously processed URLs")
            except Exception as e:
                self.logger.error(f"Error loading URL index file: {e}")
                self.processed_urls = set()
    
    def save_processed_urls(self):
        """Save processed URLs to the index file."""
        try:
            with open(self.url_index_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'last_updated': datetime.datetime.now().isoformat(),
                    'processed_urls': list(self.processed_urls)
                }, f, indent=4)
            self.logger.info(f"Saved {len(self.processed_urls)} processed URLs to index file")
        except Exception as e:
            self.logger.error(f"Error saving URL index file: {e}")
    
    def start_requests(self):
        """Generate initial requests for the content list API."""
        content_list_url = self.config['api_endpoints']['content_list']
        params = self.config['api_params']
        
        self.logger.info(f"Starting scrape for URL: {content_list_url}")
        
        yield scrapy.Request(
            url=f"{content_list_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}",
            callback=self.parse_content_list,
            headers={
                'accept': 'application/json',
                'user-agent': self.user_agent,
            }
        )
    
    def parse_content_list(self, response):
        """Parse the content list API response to extract article IDs."""
        try:
            data = json.loads(response.text)
            articles = data.get('mixedContent', [])
            
            self.logger.info(f"Found {len(articles)} articles in the content list")
            
            for article in articles:
                story_content = article.get('storyContent', {})
                article_id = story_content.get('id')
                published_date = story_content.get('publishedDate')
                
                if not article_id or not published_date:
                    continue
                
                # Skip if already processed
                if article_id in self.processed_urls:
                    self.logger.debug(f"Skipping already processed article ID: {article_id}")
                    continue
                
                # # Apply time filter if enabled
                # if self.config['time_filter']:
                #     try:
                #         # Updated format to handle milliseconds
                #         pub_datetime = datetime.datetime.strptime(published_date.split('.')[0] + 'Z', "%Y-%m-%dT%H:%M:%SZ")
                #         time_diff = datetime.datetime.now() - pub_datetime
                #         if time_diff > datetime.timedelta(hours=self.config['time_filter']):
                #             self.logger.debug(f"Skipping article older than {self.config['time_filter']} hours: {article_id}")
                #             continue
                #     except Exception as e:
                #         self.logger.error(f"Error parsing published date: {e}")
                #         continue
                
                # Check if we've reached the maximum articles
                if self.max_articles and self.articles_requested >= self.max_articles:
                    self.logger.info(f"Already requested maximum number of articles ({self.max_articles}). Stopping further parsing.")
                    return
                
                self.articles_requested += 1
                
                # Request the article detail
                article_detail_url = self.config['api_endpoints']['article_detail'].format(article_id=article_id)
                yield scrapy.Request(
                    url=f"{article_detail_url}?format=dto&type=PAC",
                    callback=self.parse_article,
                    headers={
                        'accept': 'application/json',
                        'user-agent': self.user_agent,
                    },
                    meta={'article_id': article_id}
                )
                
                # Respect the delay between requests
                time.sleep(self.request_delay)
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON response: {e}")
    
    def parse_article(self, response):
        """Parse the article detail API response to extract article data."""
        try:
            data = json.loads(response.text)
            
            # Get the complete story data using the new API endpoint
            article_id = response.meta['article_id']
            story_url = f"https://www.axios.com/api/axios-web/get-story/by-id/{article_id}"
            
            # Make a request to get the complete story data
            story_response = requests.get(
                story_url,
                headers={
                    'accept': 'application/json',
                    'user-agent': self.user_agent,
                }
            )
            
            if story_response.status_code != 200:
                self.logger.error(f"Failed to get story data for article {article_id}")
                return
            
            story_data = story_response.json()
            
            # Combine the body text from before and after keep reading sections
            body_html = story_data.get('bodyHtml', {})
            before_keep_reading = body_html.get('beforeKeepReading', '')
            after_keep_reading = body_html.get('afterKeepReading', '')
            
            # Combine and clean the body text
            full_body_html = before_keep_reading + after_keep_reading
            body_text = BeautifulSoup(full_body_html, 'html.parser').get_text(strip=True)
            
            # Clean the summary text
            summary_html = story_data.get('summary', '')
            summary_text = BeautifulSoup(summary_html, 'html.parser').get_text(strip=True)
            
            # Extract all tags
            tags = [tag.get('name') for tag in story_data.get('tags', [])]
            
            article_data = {
                'url': story_data.get('permalink'),
                'title': story_data.get('headline'),
                'description': summary_text,
                'author': story_data.get('authors', [{}])[0].get('display_name'),
                'published_date': story_data.get('published_date'),
                'source': 'axios',
                'scraped_at': datetime.datetime.now().isoformat(),
                'body': body_text,
                'tags': tags
            }
            
            # Add to processed URLs
            self.processed_urls.add(article_id)
            
            # Increment article count and check if we've reached the maximum
            self.article_count += 1
            if self.max_articles and self.article_count >= self.max_articles:
                self.logger.info(f"Reached maximum article count ({self.max_articles}). Stopping spider.")
                raise CloseSpider(reason=f"Reached maximum article count: {self.max_articles}")
            
            yield article_data
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON response: {e}")
        except Exception as e:
            self.logger.error(f"Error processing article: {e}")
    
    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Processed {len(self.processed_urls)} URLs")
        # Save processed URLs to file
        self.save_processed_urls() 