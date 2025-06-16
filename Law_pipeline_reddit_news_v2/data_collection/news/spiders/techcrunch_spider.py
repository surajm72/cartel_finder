import scrapy
import json
import time
import datetime
import os
import sys
import re
import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from scrapy.exceptions import CloseSpider

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from config.news_config import NEWS_SOURCES, OUTPUT_CONFIG

class TechcrunchSpider(scrapy.Spider):
    name = "techcrunch"
    allowed_domains = ["techcrunch.com", "www.techcrunch.com"]
    
    def __init__(self, days_back=None, start_date=None, end_date=None, 
                 sections=None, exclude_sections=None, scrape_body=None, max_articles=None, *args, **kwargs):
        super(TechcrunchSpider, self).__init__(*args, **kwargs)
        
        # Get TechCrunch config
        self.source_config = NEWS_SOURCES['techcrunch']
        self.config = self.source_config['config']
        
        # Override config with command line arguments if provided
        if days_back is not None:
            self.config['days_back'] = int(days_back)
        
        if start_date is not None and end_date is not None:
            self.config['custom_date_range']['enabled'] = True
            self.config['custom_date_range']['start_date'] = start_date
            self.config['custom_date_range']['end_date'] = end_date
        
        if sections is not None:
            self.config['section_filters']['enabled'] = True
            self.config['section_filters']['include'] = sections.split(',')
        
        if exclude_sections is not None:
            self.config['section_filters']['enabled'] = True
            self.config['section_filters']['exclude'] = exclude_sections.split(',')
        
        if scrape_body is not None:
            self.config['scrape_article_body'] = (scrape_body.lower() == 'true')
            
        # Set maximum number of articles to scrape - use parameter or config value
        if max_articles is not None:
            self.max_articles = int(max_articles)
        else:
            self.max_articles = self.config.get('max_articles')
            
        # Counter for articles scraped
        self.article_count = 0
        
        # Counter for articles requested
        self.articles_requested = 0
        
        # Set request delay
        self.request_delay = self.config['request_delay']
        
        # Set up user agent
        self.user_agent = self.config['user_agent']
        
        # Calculate date range for scraping
        self.dates_to_scrape = self._get_date_range()
        
        # Initialize processed URLs set
        self.processed_urls = set()
        
        # Create output directory if it doesn't exist
        self.output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/news/')
        )
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Collected articles store
        self.collected_articles = []
        
        # Print configuration information
        self._print_config_info()
    
    def _print_config_info(self):
        """Print detailed configuration information."""
        self.logger.info("========= TECHCRUNCH SPIDER CONFIGURATION =========")
        self.logger.info(f"Date Range: {[date.strftime('%Y-%m-%d') for date in self.dates_to_scrape]}")
        
        if self.config['custom_date_range']['enabled']:
            self.logger.info(f"Using custom date range: {self.config['custom_date_range']['start_date']} to {self.config['custom_date_range']['end_date']}")
        else:
            self.logger.info(f"Using days_back: {self.config['days_back']}")
        
        self.logger.info(f"Maximum Articles: {self.max_articles if self.max_articles else 'No limit'}")
        self.logger.info(f"Scrape Article Body: {self.config['scrape_article_body']}")
        self.logger.info(f"Request Delay: {self.config['request_delay']} seconds")
        
        if self.config['section_filters']['enabled']:
            include_sections = self.config['section_filters']['include'] or "All sections"
            exclude_sections = self.config['section_filters']['exclude'] or "None"
            self.logger.info(f"Section Filters: Include={include_sections}, Exclude={exclude_sections}")
        else:
            self.logger.info("Section Filters: Disabled (including all sections)")
        
        self.logger.info("==============================================")
    
    def _get_date_range(self):
        """Calculate the date range to scrape based on configuration."""
        dates = []
        
        if self.config['custom_date_range']['enabled']:
            # Use custom date range
            start_date = datetime.datetime.strptime(self.config['custom_date_range']['start_date'], '%Y-%m-%d').date()
            end_date = datetime.datetime.strptime(self.config['custom_date_range']['end_date'], '%Y-%m-%d').date()
            
            # Generate all dates in the range
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date)
                current_date += datetime.timedelta(days=1)
        else:
            # Use days_back setting
            days_back = self.config['days_back']
            today = datetime.date.today()
            
            # Default to yesterday if days_back is 1
            if days_back == 1:
                dates = [today - datetime.timedelta(days=1)]
            else:
                # Generate all dates in the range
                for i in range(days_back, 0, -1):  # Count backwards from days_back to 1
                    dates.append(today - datetime.timedelta(days=i))
        
        return dates
    
    def start_requests(self):
        """Generate initial requests for sitemap pages based on date range."""
        for date in self.dates_to_scrape:
            year = date.year
            month = date.month
            day = date.day
            
            # TechCrunch URL format is like: https://techcrunch.com/2025/05/03/
            url = self.source_config['sitemap_url'].format(
                year=year, month=month, day=day
            )
            
            self.logger.info(f"Starting scrape for date: {date}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_site,
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': self.user_agent,
                },
                meta={
                    'date': date.strftime('%Y-%m-%d')
                }
            )
    
    def parse_site(self, response):
        """Parse the TechCrunch date-based page to extract article listings."""
        date = response.meta.get('date')
        
        self.logger.info(f"Parsing TechCrunch articles for date: {date}")
        
        # Check if we've already reached the maximum articles
        if self.max_articles and self.articles_requested >= self.max_articles:
            self.logger.info(f"Already requested maximum number of articles ({self.max_articles}). Stopping further parsing.")
            return
        
        # Extract articles from the page - updated based on actual HTML structure
        article_items = response.css('li.wp-block-post')
        
        self.logger.info(f"Found {len(article_items)} articles on the page")
        
        articles_data = []
        
        # Process each article listing
        for article_item in article_items:
            article_data = {}
            
            # Extract URL and title from the card title link
            title_link = article_item.css('.loop-card__title a.loop-card__title-link')
            if title_link:
                article_data['url'] = title_link.attrib.get('href', '')
                article_data['title'] = title_link.css('::text').get('').strip()
            else:
                continue  # Skip if no URL/title
            
            # Skip if already processed
            if article_data['url'] in self.processed_urls:
                self.logger.debug(f"Skipping already processed URL: {article_data['url']}")
                continue
            
            # Initialize tags list
            tags = []
            
            # Extract category information and add to tags
            category = article_item.css('.loop-card__cat::text').get()
            if category:
                tags.append(category.strip())
            
            # Extract author
            author_links = article_item.css('.loop-card__author::text').getall()
            if not author_links:
                # Try to get the author text
                author_links = article_item.css('.loop-card__meta .loop-card__author::text').getall()
                if not author_links:
                    # Try to get the author from link text
                    author_links = article_item.css('.loop-card__meta .loop-card__author-list li a::text').getall()
            
            if author_links:
                article_data['author'] = ', '.join([author.strip() for author in author_links if author.strip()])
            
            # Extract published datetime
            published_time = article_item.css('time.loop-card__time::attr(datetime)').get()
            if published_time:
                article_data['published_date'] = published_time
            else:
                # If no datetime found, use the date from the URL or the scrape date
                article_data['published_date'] = date
            
            # Extract tags from post class attributes
            post_class = article_item.attrib.get('class', '')
            tag_matches = re.findall(r'tag-([a-zA-Z0-9-]+)', post_class)
            category_matches = re.findall(r'category-([a-zA-Z0-9-]+)', post_class)
            
            # Add tag matches to tags list
            if tag_matches:
                # Clean up tags by replacing hyphens with spaces
                tags.extend([tag.replace('-', ' ') for tag in tag_matches])
            
            # Add category matches to tags list
            if category_matches:
                # Clean up categories and add to tags
                tags.extend([cat.replace('-', ' ').title() for cat in category_matches])
            
            # Remove duplicates and assign to article data if tags exist
            if tags:
                article_data['tags'] = list(set(tags))
            
            # Source information
            article_data['source'] = 'techcrunch'
            
            # Add to processed URLs
            self.processed_urls.add(article_data['url'])
            
            # Check if URL matches section filters
            if self._should_process_url(article_data['url']):
                self.logger.info(f"Processing article: {article_data['title']}")
                articles_data.append(article_data)
                
                # Check if we've reached the maximum
                if self.max_articles and len(articles_data) >= self.max_articles:
                    self.logger.info(f"Reached maximum article limit. Will only process the first {self.max_articles} articles.")
                    articles_data = articles_data[:self.max_articles]
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
                    headers = {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'accept-language': 'en-US,en;q=0.9',
                        'user-agent': self.user_agent,
                    },
                    meta={'date': date, 'url': article_data['url'], 'metadata': article_data}
                )
                # Respect the delay between requests
                time.sleep(self.request_delay)
            else:
                # Just yield the metadata without fetching the full article
                yield article_data
    
    def _should_process_url(self, url):
        """Check if a URL should be processed based on section filters."""
        if not self.config['section_filters']['enabled']:
            return True
            
        # Extract the section from the URL
        # Example URL format: https://techcrunch.com/category/topic/article-title/
        url_parts = url.strip('/').split('/')
        
        # Look for the category part in the URL
        for i, part in enumerate(url_parts):
            if part == 'category' and i+1 < len(url_parts):
                section = url_parts[i+1]
                
                # Check exclude list first
                if section in self.config['section_filters']['exclude']:
                    return False
                    
                # Then check include list if it's not empty
                if self.config['section_filters']['include']:
                    return section in self.config['section_filters']['include']
                
                # If include list is empty, include all sections not in exclude list
                return True
        
        # If no category found in URL, try to use the first part after domain
        if len(url_parts) >= 4:  # domain, year, month, day, etc.
            for part in url_parts[3:]:  # Skip protocol, domain, and first slash
                if part and not part.isdigit() and part not in ['www', 'techcrunch', 'com']:
                    section = part
                    
                    # Check exclude list
                    if section in self.config['section_filters']['exclude']:
                        return False
                        
                    # Check include list
                    if self.config['section_filters']['include']:
                        return section in self.config['section_filters']['include']
                    
                    return True
        
        # Default: allow the article if we couldn't determine its section
        return True
    
    def parse_article(self, response):
        """Parse the full article page to extract content."""
        url = response.meta.get('url')
        date = response.meta.get('date')
        metadata = response.meta.get('metadata', {})
        
        self.logger.debug(f"Parsing article: {url}")
        
        # Use the metadata we already have from the listing
        article_data = metadata.copy()
        
        # Ensure all required fields are present
        if 'url' not in article_data:
            article_data['url'] = url
            
        # Source information
        article_data['source'] = 'techcrunch'
            
        # Add scraped_at if not present
        if 'scraped_at' not in article_data:
            article_data['scraped_at'] = datetime.datetime.now().isoformat()
        
        # Try to extract description/summary from the speakable-summary element
        if 'description' not in article_data:
            summary = response.css('p#speakable-summary::text').get()
            if summary:
                article_data['description'] = summary.strip()
            else:
                # Try alternative selectors for description
                summary = response.css('meta[name="description"]::attr(content)').get()
                if summary:
                    article_data['description'] = summary.strip()
        
        # Extract article body and other data from HTML
        article_body = ""
        
        # Find the main article content
        main_content = response.css('div.entry-content')
        if main_content:
            # Get all paragraphs from the main content
            paragraphs = main_content.css('p::text').getall()
            
            # If empty, try to get text from paragraph elements
            if not paragraphs or not any(p.strip() for p in paragraphs):
                paragraphs = []
                for p in main_content.css('p'):
                    paragraphs.append(''.join(p.css('::text').getall()).strip())
                    
            # Filter out empty paragraphs and join with double newlines
            article_body = '\n\n'.join([p.strip() for p in paragraphs if p.strip()])
        
        # If still no body found, use BeautifulSoup for better extraction
        if not article_body:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script, style, and ad elements
            for element in soup(['script', 'style', 'div.ad-unit']):
                element.decompose()
            
            # Find the main content
            content = soup.select_one('div.entry-content, article, div.article-content')
            if content:
                # Get all paragraphs
                paragraphs = content.find_all('p')
                article_body = '\n\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
        
        # Add body to article data if we found it
        if article_body:
            article_data['body'] = article_body
        
        # Extract additional tags from article page if available
        if 'tags' not in article_data:
            article_data['tags'] = []
        
        # Look for tags in article page
        page_tags = response.css('.article-tag a::text, .tags a::text').getall()
        if page_tags:
            existing_tags = article_data.get('tags', [])
            # Add new tags to existing ones
            for tag in page_tags:
                tag = tag.strip()
                if tag and tag not in existing_tags:
                    existing_tags.append(tag)
            
            article_data['tags'] = existing_tags
        
        # Verify/update published date if not already set
        if 'published_date' not in article_data or not article_data['published_date']:
            # Try to get the published date from the article page
            published_time = response.css('time[datetime]::attr(datetime)').get()
            if published_time:
                article_data['published_date'] = published_time
            else:
                # Use the date from the URL as a fallback
                article_data['published_date'] = date
        
        # Remove "categories" field if present
        if 'categories' in article_data:
            del article_data['categories']
            
        # Remove "date" field if present
        if 'date' in article_data:
            del article_data['date']
        
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