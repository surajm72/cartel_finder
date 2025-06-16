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

class ReutersSpider(scrapy.Spider):
    name = "reuters"
    allowed_domains = ["reuters.com", "www.reuters.com"]
    
    def __init__(self, days_back=None, start_date=None, end_date=None, 
                 sections=None, exclude_sections=None, scrape_body=None, max_articles=None, *args, **kwargs):
        super(ReutersSpider, self).__init__(*args, **kwargs)
        
        # Get Reuters config
        self.source_config = NEWS_SOURCES['reuters']
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
        self.logger.info("========= REUTERS SPIDER CONFIGURATION =========")
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
            
            # Start with page 1 for each date
            page = 1
            sitemap_url = self.source_config['sitemap_url'].format(
                year=year, month=month, day=day, page=page
            )
            
            self.logger.info(f"Starting scrape for date: {date}, page: {page}")
            yield scrapy.Request(
                url=sitemap_url,
                callback=self.parse_sitemap,
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9,en-IN;q=0.8',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                    'sec-ch-ua-arch': '"x86"',
                    'sec-ch-ua-bitness': '"64"',
                    'sec-ch-ua-full-version': '"135.0.3179.98"',
                    'sec-ch-ua-full-version-list': '"Microsoft Edge";v="135.0.3179.98", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-model': '""',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-ch-ua-platform-version': '"19.0.0"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
                },
                meta={
                    'date': date.strftime('%Y-%m-%d'),
                    'page': page,
                    'year': year,
                    'month': month,
                    'day': day
                }
            )
    
    def parse_sitemap(self, response):
        """Parse the sitemap page to extract article URLs."""
        date = response.meta.get('date')
        page = response.meta.get('page')
        year = response.meta.get('year')
        month = response.meta.get('month')
        day = response.meta.get('day')
        
        self.logger.info(f"Parsing sitemap for date: {date}, page: {page}")
        
        # Check if we've already reached the maximum articles
        if self.max_articles and self.articles_requested >= self.max_articles:
            self.logger.info(f"Already requested maximum number of articles ({self.max_articles}). Stopping further sitemap parsing.")
            return
        
        # Try to find the article URLs and metadata in JSON data first
        articles_data = self._extract_urls_from_json(response)
        
        # Count how many articles matched our filters
        matched_count = 0
        articles_to_process = []
        
        # Process each article URL
        for i, article_data in enumerate(articles_data):
            # Skip if already processed
            if article_data['url'] in self.processed_urls:
                self.logger.debug(f"Skipping already processed URL: {article_data['url']}")
                continue
            
            # Add to processed URLs
            self.processed_urls.add(article_data['url'])
            
            # Check if URL matches section filters
            if self._should_process_url(article_data['url']):
                matched_count += 1
                
                # Create full URL if it's a relative path
                if article_data['url'].startswith('/'):
                    article_data['url'] = urljoin(self.source_config['base_url'], article_data['url'])
                
                # Add this article to our processing queue
                articles_to_process.append(article_data)
                
                # Check if we've reached the maximum
                if self.max_articles and self.articles_requested + len(articles_to_process) >= self.max_articles:
                    self.logger.info(f"Reached maximum article limit in sitemap. Will only process the first {self.max_articles} articles.")
                    # Truncate the list to the maximum
                    articles_to_process = articles_to_process[:self.max_articles - self.articles_requested]
                    break
        
        self.logger.info(f"Found {len(articles_data)} URLs on page {page}, {matched_count} matched filters")
        
        # Now process the articles (respecting our max limit)
        for article_data in articles_to_process:
            self.articles_requested += 1
            
            self.logger.debug(f"Processing article URL: {article_data['url']} ({self.articles_requested}/{self.max_articles if self.max_articles else 'unlimited'})")
            
            # Add scraped_at to metadata
            article_data['scraped_at'] = datetime.datetime.now().isoformat()
            
            # Ensure source is always set for the pipeline
            article_data['source'] = 'reuters'
            
            # Request the article page if we need the body, otherwise just store the metadata
            if self.config['scrape_article_body']:
                yield scrapy.Request(
                    url=article_data['url'],
                    callback=self.parse_article,
                    headers = {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'accept-language': 'en-US,en;q=0.9,en-IN;q=0.8',
                        'priority': 'u=0, i',
                        'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                        'sec-ch-ua-arch': '"x86"',
                        'sec-ch-ua-bitness': '"64"',
                        'sec-ch-ua-full-version': '"135.0.3179.98"',
                        'sec-ch-ua-full-version-list': '"Microsoft Edge";v="135.0.3179.98", "Not-A.Brand";v="8.0.0.0", "Chromium";v="135.0.7049.115"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-model': '""',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-ch-ua-platform-version': '"19.0.0"',
                        'sec-fetch-dest': 'document',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-site': 'none',
                        'sec-fetch-user': '?1',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
                    },
                    meta={'date': date, 'url': article_data['url'], 'metadata': article_data}
                )
                # Respect the delay between requests
                time.sleep(self.request_delay)
            else:
                # Just yield the metadata without fetching the full article
                yield article_data
        
        # Check if we should continue to the next page
        if len(articles_data) > 0 and (self.max_articles is None or self.articles_requested < self.max_articles):
            next_page = page + 1
            next_page_url = self.source_config['sitemap_url'].format(
                year=year, month=month, day=day, page=next_page
            )
            
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_sitemap,
                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9,en-IN;q=0.8',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                    'sec-ch-ua-arch': '"x86"',
                    'User-Agent': self.user_agent
                    },
                meta={
                    'date': date,
                    'page': next_page,
                    'year': year,
                    'month': month,
                    'day': day
                }
            )
        else:
            if self.max_articles and self.articles_requested >= self.max_articles:
                self.logger.info(f"Reached maximum article count ({self.max_articles}). Stopping further requests.")
    
    def _extract_urls_from_json(self, response):
        """Extract article URLs and metadata from JSON data embedded in the page."""
        articles_data = []
        
        # Look for script tags containing application/javascript or application/ld+json
        script_tags = ''.join(response.xpath('//script[@id="fusion-metadata"]/text()').getall())
        json_content = script_tags.replace(";Fusion.spa=false;Fusion.spaEnabled=false;</script>", "").replace('window.Fusion=window.Fusion||{};Fusion.arcSite="reuters";Fusion.contextPath="/pf";Fusion.mxId="00000000";Fusion.deployment="280";Fusion.globalContent=', "")
        
        # Fix common JSON issues
        # 1. Replace control characters
        json_content = re.sub(r'[\x00-\x1F\x7F]', ' ', json_content)
        
        # 2. Fix escaped quotes followed by commas or closing brackets
        json_content = re.sub(r'\\"(,|\]|\})', r'"\\1', json_content)
        
        # 3. Fix unescaped newlines
        json_content = json_content.replace('\n', '\\n').replace('\r', '\\r')
        json_match = re.search(r'{"data":{"statusCode":200,"message":"Success","result":{"pagination":', json_content)
        
        if json_match:
            try:
                # Find the JSON object start and end
                start_pos = json_content.find('{', json_match.start())
                json_data = self._extract_json_object(json_content[start_pos:])
                
                # Parse the JSON
                data = json.loads(json_data)
                
                # Extract article URLs and metadata based on their structure
                if 'data' in data and 'result' in data['data'] and 'articles' in data['data']['result']:
                    for article in data['data']['result']['articles']:
                        article_data = {}
                        
                        # Extract URL
                        if 'canonical_url' in article:
                            article_data['url'] = article['canonical_url']
                        else:
                            continue  # Skip if no URL
                        
                        # Extract title
                        if 'title' in article:
                            article_data['title'] = article['title']
                        elif 'basic_headline' in article:
                            article_data['title'] = article['basic_headline']
                        elif 'web' in article:
                            article_data['title'] = article['web']
                            
                        # Extract published date
                        if 'published_time' in article:
                            article_data['published_date'] = article['published_time']
                            
                        # Extract description
                        if 'description' in article:
                            article_data['description'] = article['description']
                            
                        # Extract author
                        if 'authors' in article and article['authors']:
                            authors = []
                            for author in article['authors']:
                                if 'name' in author:
                                    authors.append(author['name'])
                            if authors:
                                article_data['author'] = ', '.join(authors)
                        
                        # Extract tags/categories
                        tags = []
                        # From kicker names
                        if 'kicker' in article and 'names' in article['kicker']:
                            tags.extend(article['kicker']['names'])
                        
                        # From primary tag
                        if 'primary_tag' in article and 'text' in article['primary_tag']:
                            tags.append(article['primary_tag']['text'])
                            
                        # From ad topics
                        if 'ad_topics' in article:
                            tags.extend(article['ad_topics'])
                            
                        if tags:
                            article_data['tags'] = list(set(tags))  # Remove duplicates
                        
                        # Always set the source - CRITICAL for pipeline
                        article_data['source'] = 'reuters'
                        
                        # Add to articles data
                        articles_data.append(article_data)
            except Exception as e:
                self.logger.error(f"Error parsing JSON data: {e}")
        
        # Return both the URLs for pagination and the full article data
        return articles_data
    
    def _extract_json_object(self, text, start_pos=0):
        """Extract a complete JSON object from a string starting at a specific position."""
        stack = []
        in_string = False
        escape_char = False
        
        for i in range(start_pos, len(text)):
            char = text[i]
            
            # Handle string literals
            if char == '"' and not escape_char:
                in_string = not in_string
            
            # Skip processing special characters inside strings
            if in_string:
                escape_char = char == '\\' and not escape_char
                continue
                
            # Handle brackets
            if char == '{':
                stack.append(char)
            elif char == '}':
                if stack and stack[-1] == '{':
                    stack.pop()
                else:
                    # Unmatched closing bracket
                    return None
            
            # Check if we've found a complete JSON object
            if i >= start_pos and not stack:
                return text[start_pos:i+1]
            
        # If we get here, no complete JSON object was found
        return None
    
    def _should_process_url(self, url):
        """Check if a URL should be processed based on section filters."""
        if not self.config['section_filters']['enabled']:
            return True
            
        # Extract the section from the URL
        # Example URL format: /business/finance/some-article-title-2025-05-02/
        url_parts = url.strip('/').split('/')
        
        if len(url_parts) >= 1:
            # The first part is usually the section
            section = url_parts[0]
            
            # Check exclude list first
            if section in self.config['section_filters']['exclude']:
                return False
                
            # Then check include list if it's not empty
            if self.config['section_filters']['include']:
                return section in self.config['section_filters']['include']
            
            # If include list is empty, include all sections not in exclude list
            return True
            
        return True
    
    def parse_article(self, response):
        """Parse the full article page to extract content."""
        url = response.meta.get('url')
        date = response.meta.get('date')
        metadata = response.meta.get('metadata', {})
        
        self.logger.debug(f"Parsing article: {url}")
        
        # Use the metadata we already have from the JSON listing
        article_data = metadata
        
        # Ensure all required fields are present
        if 'url' not in article_data:
            article_data['url'] = url
            
        # Always set the source field - critical for the pipeline
        article_data['source'] = 'reuters'
            
        if 'scraped_at' not in article_data:
            article_data['scraped_at'] = datetime.datetime.now().isoformat()
        
        # If we don't have the body, try HTML parsing
        if not article_data.get('body'):
            html_data = self._extract_article_data_from_html(response)
            
            # Only update fields that weren't found in JSON
            for key, value in html_data.items():
                if not article_data.get(key) and value:
                    article_data[key] = value
        
        # Remove any "date" field if present to standardize with TechCrunch
        if 'date' in article_data:
            del article_data['date']
            
        # Ensure we have a published_date field
        if 'published_date' not in article_data and date:
            article_data['published_date'] = date
                    
        # Increment article count and check if we've reached the maximum
        self.article_count += 1
        if self.max_articles and self.article_count >= self.max_articles:
            self.logger.info(f"Reached maximum article count ({self.max_articles}). Stopping spider.")
            # Close the spider gracefully
            raise scrapy.exceptions.CloseSpider(reason=f"Reached maximum article count: {self.max_articles}")
        
        yield article_data
    
    def _extract_article_data_from_html(self, response):
        """Extract article data from HTML structure."""
        article_data = {
            'body': None,
        }
        
        # Article body
        # Try to find the main article content
        article_body = ""
        
        # Reuters usually has article paragraphs in specific containers
        paragraphs = response.xpath('//div[contains(@data-testid,"paragraph")]//text()').getall()
        
        if paragraphs:
            article_body = '\n\n'.join([p.strip() for p in paragraphs if p.strip()])
            
        # If no paragraphs found, try a more generic approach
        if not article_body:
            # Use BeautifulSoup for better text extraction
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style", "header", "footer", "nav"]):
                script.decompose()
                
            # Find the main content area
            main_content = soup.select_one('article, .StandardArticleBody, .ArticleBody, .article-body')
            
            if main_content:
                article_body = main_content.get_text(separator='\n\n', strip=True)
            else:
                # Fallback: just get the main content area text
                article_body = soup.select_one('main, #content, .content').get_text(separator='\n\n', strip=True)
                
        article_data['body'] = article_body
        
        return article_data
        
    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Processed {len(self.processed_urls)} URLs") 