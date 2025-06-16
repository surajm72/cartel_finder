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
from dateutil import parser as date_parser

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from config.news_config import NEWS_SOURCES, OUTPUT_CONFIG

class ApnewsSpider(scrapy.Spider):
    name = "apnews"
    allowed_domains = ["apnews.com"]
    
    def __init__(self, max_articles=None, max_pages=None, 
                 scrape_body=None, time_filter=None, 
                 source_urls=None, *args, **kwargs):
        super(ApnewsSpider, self).__init__(*args, **kwargs)
        
        # Get AP News config
        self.source_config = NEWS_SOURCES['apnews']
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
            'apnews_url_index.json'
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
        self.logger.info("========= AP NEWS SPIDER CONFIGURATION =========")
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
        """Generate initial requests for AP News top stories."""
        base_url = self.config['base_url']
        
        self.logger.info(f"Starting scrape for URL: {base_url}")
        
        yield scrapy.Request(
            url=base_url,
            callback=self.parse_article_list,
            headers={
                'User-Agent': self.user_agent
            }
        )
    
    def parse_article_list(self, response):
        """Parse the article list page to extract article URLs."""
        # Extract article links using CSS selector for AP News articles
        article_links = response.xpath('//a[@class="Link "]/@href').getall()
        
        self.logger.info(f"Found {len(article_links)} articles on the page")
        
        for article_link in article_links:
            # Skip if already processed
            if article_link in self.processed_urls:
                self.logger.debug(f"Skipping already processed article: {article_link}")
                continue
            
            # Check if we've reached the maximum articles
            if self.max_articles and self.article_count >= self.max_articles:
                self.logger.info(f"Reached maximum article count ({self.max_articles}). Stopping spider.")
                return
            
            # Build full URL
            article_url = urljoin(self.source_config['base_url'], article_link)
            
            yield scrapy.Request(
                url=article_url,
                callback=self.parse_article,
                headers={
                    'User-Agent': self.user_agent
                },
                meta={'article_url': article_url}
            )
            
            # Respect the delay between requests
            time.sleep(self.request_delay)
    
    def parse_article(self, response):
        """Parse the article page to extract article data."""
        try:
            # Title
            title = response.css('h1::text').get()

            # Authors (can be multiple, both <a> and <span> inside .Page-authors)
            authors = response.css('.Page-authors a.Link::text').getall()
            authors += response.css('.Page-authors span.Link::text').getall()
            author = ', '.join([a.strip() for a in authors if a.strip()])

            # Published date (from meta tag)
            date_str = response.css('meta[property="article:published_time"]::attr(content)').get()
            published_date = None
            if date_str:
                try:
                    # Parse the date string and convert to ISO 8601 format
                    parsed_date = date_parser.parse(date_str)
                    published_date = parsed_date.isoformat()
                except Exception as e:
                    self.logger.error(f"Error parsing date {date_str}: {e}")
                    # Fallback to the old method if meta tag parsing fails
                    date_str = response.css('.Page-dateModified [data-date]::text').get()
                    if date_str:
                        try:
                            parsed_date = date_parser.parse(date_str)
                            published_date = parsed_date.isoformat()
                        except Exception as e:
                            self.logger.error(f"Error parsing fallback date {date_str}: {e}")

            # Body: all <p> inside .RichTextStoryBody.RichTextBody, skip ads/figcaption/etc.
            paragraphs = response.css('.RichTextStoryBody.RichTextBody p::text').getall()
            body_text = ' '.join([p.strip() for p in paragraphs if p.strip()])

            # Description: first paragraph
            #description = paragraphs[0].strip() if paragraphs else ''
            description = ''

            # Tags: from breadcrumbs
            tags = response.css('.Page-breadcrumbs a.Link::text').getall()

            article_data = {
                'url': response.meta['article_url'],
                'title': title,
                'description': description,
                'author': author,
                'published_date': published_date,
                'source': 'apnews',
                'scraped_at': datetime.datetime.now().isoformat(),
                'body': body_text if self.config['scrape_article_body'] else None,
                'tags': tags
            }

            # Add to processed URLs
            self.processed_urls.add(response.meta['article_url'])

            # Increment article count
            self.article_count += 1

            yield article_data

        except Exception as e:
            self.logger.error(f"Error processing article: {e}")
    
    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Processed {len(self.processed_urls)} URLs")
        # Save processed URLs to file
        self.save_processed_urls() 