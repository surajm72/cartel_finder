import scrapy
import json
import time
import datetime
import os
import sys
import re
import logging
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from scrapy.exceptions import CloseSpider

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from config.news_config import NEWS_SOURCES, OUTPUT_CONFIG

class PatchSpider(scrapy.Spider):
    name = "patch"
    allowed_domains = ["patch.com"]
    
    def __init__(self, max_articles=None, max_pages=None, 
                 scrape_body=None, time_filter=None, 
                 source_urls=None, *args, **kwargs):
        super(PatchSpider, self).__init__(*args, **kwargs)
        
        # Get Patch config
        self.source_config = NEWS_SOURCES['patch']
        self.config = self.source_config['config']
        
        # Override config with command line arguments if provided
        if max_articles is not None:
            self.max_articles = int(max_articles)
        else:
            self.max_articles = self.config.get('max_articles')
            
        if max_pages is not None:
            self.max_pages = int(max_pages)
        else:
            self.max_pages = self.config.get('max_pages', 3)
            
        if scrape_body is not None:
            self.config['scrape_article_body'] = (scrape_body.lower() == 'true')
            
        if time_filter is not None:
            self.config['time_filter'] = int(time_filter)
        
        # Set URLs to scrape
        if source_urls is not None:
            self.source_urls = source_urls.split(',')
        else:
            self.source_urls = self.source_config.get('source_urls', [])
            
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
            'patch_url_index.json'
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
        self.logger.info("========= PATCH SPIDER CONFIGURATION =========")
        self.logger.info(f"Source URLs: {self.source_urls}")
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
        """Generate initial requests for source URLs."""
        for url in self.source_urls:
            self.logger.info(f"Starting scrape for URL: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_list_page,
                headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': self.user_agent,
                },
                meta={
                    'source_url': url,
                    'page': 1
                }
            )
    
    def parse_list_page(self, response):
        """Parse the article listing page to extract article data."""
        source_url = response.meta.get('source_url')
        current_page = response.meta.get('page', 1)
        
        self.logger.info(f"Parsing Patch.com listing page {current_page}: {response.url}")
        
        # Check if we've already reached the maximum articles
        if self.max_articles and self.articles_requested >= self.max_articles:
            self.logger.info(f"Already requested maximum number of articles ({self.max_articles}). Stopping further parsing.")
            return
        
        # Extract articles from the page - based on the HTML structure provided
        article_items = response.css('main.page__main article')
        
        self.logger.info(f"Found {len(article_items)} articles on the page")
        
        articles_data = []
        
        # Process each article listing
        for article_item in article_items:
            article_data = {}
            
            # Extract URL and title
            title_link = article_item.css('h2 a')
            if title_link:
                article_data['url'] = title_link.attrib.get('href', '')
                if not article_data['url'].startswith('http'):
                    article_data['url'] = urljoin('https://patch.com', article_data['url'])
                article_data['title'] = title_link.css('::text').get('').strip()
            else:
                continue  # Skip if no URL/title
            
            # Skip if already processed
            if article_data['url'] in self.processed_urls:
                self.logger.debug(f"Skipping already processed URL: {article_data['url']}")
                continue
            
            # Extract description
            description = article_item.css('p::text').get()
            if description:
                article_data['description'] = description.strip()
            
            # Extract author
            author = article_item.css('strong::text').get()
            if author:
                article_data['author'] = author.strip()
            
            # Extract published time
            published_time = article_item.css('time::attr(datetime)').get()
            if published_time:
                article_data['published_date'] = published_time
                
                # Apply time filter if enabled
                if self.config['time_filter']:
                    try:
                        # Parse the datetime string
                        pub_datetime = datetime.datetime.strptime(published_time, "%Y-%m-%dT%H:%M:%SZ")
                        # Calculate time difference
                        time_diff = datetime.datetime.now() - pub_datetime
                        # Skip if article is older than the time filter
                        if time_diff > datetime.timedelta(hours=self.config['time_filter']):
                            self.logger.debug(f"Skipping article older than {self.config['time_filter']} hours: {article_data['title']}")
                            continue
                    except Exception as e:
                        self.logger.error(f"Error parsing published date: {e}")
            
            # Source information
            article_data['source'] = 'patch'
            
            # Add to processed URLs
            self.processed_urls.add(article_data['url'])
            
            # Process this article
            self.logger.info(f"Processing article: {article_data['title']}")
            articles_data.append(article_data)
            
            # Check if we've reached the maximum
            if self.max_articles and len(articles_data) + self.articles_requested >= self.max_articles:
                self.logger.info(f"Reached maximum article limit. Will only process the first {self.max_articles} articles.")
                remaining = self.max_articles - self.articles_requested
                articles_data = articles_data[:remaining]
                break
        
        # Now process the articles that matched our filters
        for article_data in articles_data:
            self.articles_requested += 1
            
            self.logger.debug(f"Processing article URL: {article_data['url']} ({self.articles_requested}/{self.max_articles if self.max_articles else 'unlimited'})")
            
            # Add scraped_at to metadata
            article_data['scraped_at'] = datetime.datetime.now().isoformat()
            
            # Request the article page if we need the body, otherwise just yield the metadata
            if self.config['scrape_article_body']:
                yield scrapy.Request(
                    url=article_data['url'],
                    callback=self.parse_article,
                    headers={
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                        'accept-language': 'en-US,en;q=0.9',
                        'user-agent': self.user_agent,
                    },
                    meta={'metadata': article_data}
                )
                # Respect the delay between requests
                time.sleep(self.request_delay)
            else:
                # Just yield the metadata without fetching the full article
                yield article_data
        
        # Check if we should move to the next page
        if current_page < self.max_pages and (not self.max_articles or self.articles_requested < self.max_articles):
            # Construct next page URL
            next_page_url = f"{source_url}?page={current_page + 1}"
            
            self.logger.info(f"Moving to next page: {next_page_url}")
            
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_list_page,
                headers={
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': self.user_agent,
                },
                meta={
                    'source_url': source_url,
                    'page': current_page + 1
                }
            )
    
    def parse_article(self, response):
        """Parse the full article page to extract content."""
        metadata = response.meta.get('metadata', {})
        url = metadata.get('url')
        
        self.logger.debug(f"Parsing article: {url}")
        
        # Use the metadata we already have from the listing
        article_data = metadata.copy()
        
        # Extract article body from the article content
        article_body = ""
        
        # Use the main content selector provided
        main_content = response.css('.page__main article p')
        if main_content:
            # Get all paragraph text
            paragraphs = main_content.css('::text').getall()
            
            # Filter out empty paragraphs and join with double newlines
            article_body = '\n\n'.join([p.strip() for p in paragraphs if p.strip()])
        
        # If still no body found, use BeautifulSoup for better extraction
        if not article_body:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script, style, and ad elements
            for element in soup(['script', 'style', 'div.ad-unit']):
                element.decompose()
            
            # Find the main content
            content = soup.select('.page__main article')
            if content:
                # Get all paragraphs
                paragraphs = [p.get_text(strip=True) for p in content[0].find_all('p') if p.get_text(strip=True)]
                article_body = '\n\n'.join(paragraphs)
        
        # Add body to article data if we found it
        if article_body:
            article_data['body'] = article_body
        
        # Extract tags from topic labels in the article detail page
        topic_label = response.css('.page__main article h6 a::text').get()
        if topic_label:
            # Create tags list if it doesn't exist
            article_data['tags'] = [topic_label]
            
        # Increment article count and check if we've reached the maximum
        self.article_count += 1
        if self.max_articles and self.article_count >= self.max_articles:
            self.logger.info(f"Reached maximum article count ({self.max_articles}). Stopping spider.")
            # Close the spider gracefully
            raise CloseSpider(reason=f"Reached maximum article count: {self.max_articles}")
        
        yield article_data
    
    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Processed {len(self.processed_urls)} URLs")
        # Save processed URLs to file
        self.save_processed_urls() 