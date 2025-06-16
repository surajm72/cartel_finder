import scrapy
import json
import time
import datetime
import os
import sys
from urllib.parse import urljoin
import re
import requests
from bs4 import BeautifulSoup
import unicodedata
import html
import trafilatura  # Add this import for URL content extraction

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from config.reddit_config import SUBREDDITS, SCRAPING_CONFIG, OUTPUT_CONFIG

#######################
# RedditSpider Class #
#######################

class RedditSpider(scrapy.Spider):
    name = "reddit"
    allowed_domains = ["reddit.com", "www.reddit.com", "old.reddit.com"]
    
    #########################
    # Initialization Setup #
    #########################
    
    def __init__(self, subreddit=None, *args, **kwargs):
        super(RedditSpider, self).__init__(*args, **kwargs)
        self.subreddits = [subreddit] if subreddit else SUBREDDITS
        self.request_delay = SCRAPING_CONFIG.get('request_delay', 2.0)
        self.posts_per_subreddit = SCRAPING_CONFIG.get('posts_per_subreddit', 100)
        self.comments_per_post = SCRAPING_CONFIG.get('comments_per_post', 25)
        self.sort_method = SCRAPING_CONFIG.get('sort_method', 'hot')
        self.time_filter = SCRAPING_CONFIG.get('time_filter', 'month')
        self.user_agent = SCRAPING_CONFIG.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
        
        # Load previously processed URLs from index file
        self.index_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            OUTPUT_CONFIG.get('output_folder', 'raw_data/reddit'),
            'processed_urls_index.json'
        )
        self.processed_urls = self.load_processed_urls()
        self.logger.info(f"Loaded {len(self.processed_urls)} previously processed URLs from index file")
        
        # Configure the base URLs
        if self.sort_method == 'top':
            self.base_urls = [f"https://old.reddit.com/r/{sub}/top/?t={self.time_filter}" for sub in self.subreddits]
        else:
            self.base_urls = [f"https://old.reddit.com/r/{sub}/{self.sort_method}/" for sub in self.subreddits]
        
        self.logger.info(f"Spider initialized with subreddits: {self.subreddits}")
        self.logger.info(f"Using sort method: {self.sort_method}")
    
    ################################
    # URL Processing and Tracking #
    ################################
    
    def load_processed_urls(self):
        """Load previously processed URLs from the index file."""
        processed_urls = set()
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.index_file_path), exist_ok=True)
        
        if os.path.exists(self.index_file_path):
            try:
                with open(self.index_file_path, 'r', encoding='utf-8') as f:
                    processed_urls = set(json.load(f))
                    self.logger.info(f"Loaded {len(processed_urls)} URLs from index file")
            except Exception as e:
                self.logger.error(f"Error loading processed URLs: {str(e)}")
        else:
            self.logger.info(f"No index file found at {self.index_file_path}, creating a new one")
            
        return processed_urls
    
    def save_processed_urls(self):
        """Save processed URLs to the index file in append mode."""
        try:
            # Get existing URLs if the file exists
            existing_urls = set()
            if os.path.exists(self.index_file_path):
                try:
                    with open(self.index_file_path, 'r', encoding='utf-8') as f:
                        existing_urls = set(json.load(f))
                except Exception as e:
                    self.logger.error(f"Error reading existing URLs: {str(e)}")
            
            # Combine existing and new URLs
            all_urls = existing_urls.union(self.processed_urls)
            
            # Save the combined set back to the file
            with open(self.index_file_path, 'w', encoding='utf-8') as f:
                json.dump(list(all_urls), f, indent=4)
            
            self.logger.info(f"Saved {len(all_urls)} URLs to index file (added {len(self.processed_urls - existing_urls)} new URLs)")
        except Exception as e:
            self.logger.error(f"Error saving processed URLs: {str(e)}")
    
    #########################
    # Main Scraping Flow #
    #########################
    
    def start_requests(self):
        for url in self.base_urls:
            subreddit = re.search(r'/r/([^/]+)', url).group(1)
            self.logger.info(f"Starting to scrape subreddit: {subreddit}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_subreddit,
                headers={'User-Agent': self.user_agent},
                meta={'subreddit': subreddit, 'page': 1}
            )
    
    def parse_subreddit(self, response):
        subreddit = response.meta.get('subreddit')
        page = response.meta.get('page', 1)
        
        # Extract posts from the page
        posts = response.css('div.thing.link')
        self.logger.info(f"Found {len(posts)} posts on page {page} for r/{subreddit}")
        
        posts_scraped = (page - 1) * 25  # Reddit typically shows 25 posts per page
        encountered_processed_url = False
        processed_posts_count = 0
        
        for post in posts:
            posts_scraped += 1
            if posts_scraped > self.posts_per_subreddit:
                self.logger.info(f"Reached limit of {self.posts_per_subreddit} posts for r/{subreddit}")
                break
                
            post_id = post.css('::attr(data-fullname)').get()
            post_url = post.css('a.title::attr(href)').get()
            
            # Make sure we have a full URL
            if post_url and not post_url.startswith('http'):
                post_url = urljoin(response.url, post_url)
                
            # Skip external links and go directly to comments
            if post_url and '/comments/' not in post_url:
                permalink = post.css('a.comments::attr(href)').get()
                if permalink:
                    post_url = permalink
            
            # Check if this URL has been processed before
            if post_url in self.processed_urls:
                processed_posts_count += 1
                self.logger.info(f"Skipping previously processed URL: {post_url}")
                # If we've processed several posts in sequence, assume we've reached
                # the end of new content for this subreddit
                if processed_posts_count >= 3:  # Stop after 3 consecutive processed posts
                    self.logger.info(f"Found multiple processed posts in sequence for r/{subreddit}. Moving to next subreddit.")
                    encountered_processed_url = True
                    break
                # Otherwise continue checking other posts
                continue
            
            processed_posts_count = 0  # Reset the counter when we find a new post
            
            if post_url:
                # Add this URL to the set of processed URLs
                self.processed_urls.add(post_url)
                
                # Extract basic post data to pass to the comments page
                post_data = {
                    'id': post_id,
                    'title': self.encode_unicode(post.css('a.title::text').get()),
                    'url': post_url,
                    'author': post.css('a.author::text').get(),
                    'score': post.css('div.score.unvoted::attr(title)').get(),
                    'num_comments': post.css('a.comments::text').re_first(r'(\d+)\s+comments'),
                    'subreddit': subreddit,
                    'created': post.css('time::attr(datetime)').get(),
                }
                
                # Parse created timestamp to created_utc right here
                created_timestamp = post.css('time::attr(datetime)').get()
                if created_timestamp:
                    try:
                        # Convert ISO format to UTC timestamp
                        dt = datetime.datetime.fromisoformat(created_timestamp.replace('Z', '+00:00'))
                        post_data['created_utc'] = int(dt.timestamp())
                    except Exception as e:
                        self.logger.error(f"Error parsing timestamp: {e}")
                        post_data['created_utc'] = None
                else:
                    post_data['created_utc'] = None
                
                # Detect if this is a link post to external site
                domain = post.css('span.domain a::text').get()
                is_external_link = domain and 'self.' not in domain
                
                # Store the original post URL if it's an external link
                if is_external_link:
                    # Get the actual external URL
                    external_url = post.css('a.title::attr(href)').get()
                    if external_url and not 'redd.it' in external_url and not external_url.startswith('/r/'):
                        post_data['external_url'] = external_url
                
                # Go to the comments page
                yield scrapy.Request(
                    url=post_url,
                    callback=self.parse_post,
                    headers={'User-Agent': self.user_agent},
                    meta={'post_data': post_data},
                    dont_filter=True
                )
                
                # Respect the delay between requests
                time.sleep(self.request_delay)
        
        # If we encountered processed URLs, stop scraping this subreddit but don't close the spider
        if encountered_processed_url:
            self.logger.info(f"Stopping scraping for r/{subreddit} as we've reached previously processed content")
            # We don't close the spider here, just return to stop processing this subreddit
            return
        
        # Go to the next page if we haven't scraped enough posts
        if posts_scraped < self.posts_per_subreddit:
            next_page = response.css('span.next-button a::attr(href)').get()
            if next_page:
                self.logger.info(f"Going to next page {page+1} for r/{subreddit}")
                yield scrapy.Request(
                    url=next_page,
                    callback=self.parse_subreddit,
                    headers={'User-Agent': self.user_agent},
                    meta={'subreddit': subreddit, 'page': page + 1},
                    dont_filter=True
                )
    
    ############################
    # Content Processing Utils #
    ############################
    
    def fetch_and_clean_url_content(self, url):
        """
        Fetch content from a URL and extract the cleaned text
        Returns the cleaned text from the URL
        """
        try:
            self.logger.info(f"Fetching content from external URL: {url}")
            # Use trafilatura to extract clean text from the URL
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                # Extract the main content
                text = trafilatura.extract(downloaded, include_links=False, 
                                        include_images=False, 
                                        include_tables=False,
                                        output_format='txt')
                
                if text:
                    # Encode the text to handle emojis and special characters
                    return self.encode_unicode(text)
                
            # Fallback method if trafilatura fails
            headers = {
                'User-Agent': self.user_agent
            }
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style", "header", "footer", "nav"]):
                    script.decompose()
                    
                # Get text and clean it
                text = soup.get_text(separator='\n')
                # Clean lines and remove excess whitespace
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = '\n'.join(chunk for chunk in chunks if chunk)
                
                return self.encode_unicode(text[:5000])  # Limit to 5000 chars to prevent excessively large content
                
            return ""
        except Exception as e:
            self.logger.error(f"Error fetching content from URL {url}: {e}")
            return ""
    
    def encode_unicode(self, text):
        """
        Encode text with emojis into unicode format that can be decoded later
        """
        return text
        # if not text:
        #     return ""
        
        # try:
        #     # Unescape HTML entities first
        #     unescaped = html.unescape(text)
            
        #     # Normalize unicode characters using NFC normalization
        #     normalized = unicodedata.normalize('NFC', unescaped)
            
        #     # Remove zero-width spaces and other invisible characters
        #     normalized = re.sub(r'[\u200B-\u200D\uFEFF]', '', normalized)
            
        #     # Create a new string with emoji characters encoded as \uXXXX
        #     result = ""
        #     for char in normalized:
        #         if ord(char) > 127:  # Non-ASCII character
        #             # For characters that need surrogate pairs (like many emojis)
        #             if ord(char) > 0xFFFF:
        #                 # Use the explicit \U00XXXXXX format for characters outside the BMP
        #                 char_encoded = f"\\U{ord(char):08x}"
        #             else:
        #                 # Use the \uXXXX format for characters in the BMP
        #                 char_encoded = f"\\u{ord(char):04x}"
        #             result += char_encoded
        #         else:
        #             # Keep ASCII characters as is
        #             result += char
            
        #     return result
        # except Exception as e:
        #     self.logger.error(f"Error encoding unicode: {e}")
        #     # Emergency fallback - just keep ASCII characters
        #     return re.sub(r'[^\x00-\x7F]+', ' ', text if text else '')
    
    def is_video_url(self, url):
        """
        Check if URL is likely a video URL
        """
        if not url:
            return False
            
        video_patterns = [
            r'\.mp4(\?|$)',
            r'\.webm(\?|$)',
            r'\.mov(\?|$)',
            r'\.m3u8(\?|$)',
            r'/HLSPlaylist\.m3u8',
            r'v\.redd\.it',
            r'youtube\.com/watch',
            r'youtu\.be/',
            r'vimeo\.com/'
        ]
        
        for pattern in video_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return True
        
        return False
    
    #######################
    # Post Processing #
    #######################
    
    def parse_post(self, response):
        post_data = response.meta.get('post_data', {})

        if not post_data.get('score'):
            post_data['score'] =  response.css('div.score span.number::text').get()
            
        if not post_data.get('num_comments'):
            post_data['num_comments'] = response.css('a.comments::text').get().replace('comments','').replace('comment','').strip()
            if post_data['num_comments'] == '':
                post_data['num_comments'] = 0
        # Extract the full post data including the body text
        post_body = response.css('div.thing.self div.md') or response.css('div.expando div.md')
        post_body_text = None
        if post_body:
            # Get the HTML content and parse it with BeautifulSoup to extract all text including links
            post_body_html = post_body.get()
            if post_body_html:
                soup = BeautifulSoup(post_body_html, 'html.parser')
                post_body_text = soup.get_text(separator=' ', strip=True)
        
        # Check for media: images or videos
        media_urls = []
        
        # Look for images in the post content (old.reddit.com format)
        images = response.xpath('//a[contains(@class, "post-link")]/@href').getall()
        if images:
            media_urls.extend(images)
        
        # Look for videos in the post content
        videos = response.xpath('//div[contains(@class, "portrait")]/@data-seek-preview-url').getall()
        if videos:
            media_urls.extend(videos)
        
        # Determine content type and handle external links
        content_type = "text"  # Default
        content = None
        body_text = post_body_text
        
        # Check for external link first
        external_url = post_data.get('external_url')
        
        # Classify content type based on what we found
        if external_url:
            if self.is_video_url(external_url):
                content_type = "video"
                content = external_url
                body_text = None
            elif external_url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
                content_type = "image"
                content = external_url
                body_text = None
            else:
                # It's a regular link
                content_type = "link"
                content = external_url
                # Fetch the content from the external URL
                body_text = self.fetch_and_clean_url_content(external_url)
        elif videos or any(self.is_video_url(url) for url in media_urls):
            # Video post
            content_type = "video"
            video_urls = [url for url in media_urls if self.is_video_url(url)]
            if not video_urls and videos:
                video_urls = videos
            if video_urls:
                content = video_urls[0]  # Use the first video
            else:
                content = "[VIDEO CONTENT]"
            body_text = None
        elif images or [url for url in media_urls if not self.is_video_url(url)]:
            # Image post
            image_urls = [url for url in media_urls if not self.is_video_url(url)]
            if not image_urls and images:
                image_urls = images
                
            if len(image_urls) > 1:
                content_type = "image_gallery"
                content = "|".join(image_urls)
            else:
                content_type = "image"
                content = image_urls[0] if image_urls else "[IMAGE CONTENT]"
            body_text = None
        elif post_body_text:
            # Text post
            content_type = "text"
            content = None
            body_text = post_body_text
        else:
            # Unknown or empty post
            content_type = "unknown"
            content = None
            body_text = ""
        
        # If we have both text and media, it's mixed content
        if post_body_text and media_urls:
            content_type = "mixed"
            content = "|".join(media_urls)  # Join all media URLs
            body_text = post_body_text  # Keep the text content
        
        # Complete post data with our new structure
        complete_post_data = {
            **post_data,
            'content_type': content_type,
            'content': content,
            'body_text': self.encode_unicode(body_text),
            'created_utc': post_data['created_utc'],
            'comments': []
        }
        
        # Create a mapping of comment IDs to their hierarchical structure
        comment_hierarchy = self.extract_comment_hierarchy(response)
        
        # Log details about the comment hierarchy
        self.logger.info(f"Found {len(comment_hierarchy)} total comments for post {post_data.get('id')}")
        top_level_count = sum(1 for data in comment_hierarchy.values() if data['is_top_level'])
        self.logger.info(f"Of which {top_level_count} are top-level comments")

        if not comment_hierarchy:
            self.logger.warning(f"No comments found for post {post_data.get('id')} - URL: {response.url}")
            # Check if we see a comment area at all
            if not response.css('div.commentarea'):
                self.logger.error(f"No comment area found on the page - URL: {response.url}")
            else:
                # Try direct CSS selectors to debug
                all_comments = response.css('div.thing.comment')
                self.logger.info(f"Direct CSS selector found {len(all_comments)} comments")
        
        # Extract top-level comments first, then extract replies recursively
        for comment_id, comment_data in comment_hierarchy.items():
            # Only process top-level comments directly
            if comment_data['is_top_level']:
                # Add post_id to each comment
                comment_obj = comment_data['comment_obj']
                comment_obj['post_id'] = post_data.get('id')
                
                # Recursively add replies
                if comment_data['replies']:
                    comment_obj['replies'] = self.build_replies_tree(
                        comment_data['replies'], 
                        comment_hierarchy, 
                        post_data.get('id')
                    )
                else:
                    comment_obj['replies'] = []
                    
                # Add to the complete post data
                complete_post_data['comments'].append(comment_obj)
        
        yield complete_post_data
    
    ##########################
    # Comment Processing #
    ##########################
    
    def extract_comment_hierarchy(self, response):
        """
        Extract all comments and build a mapping of their hierarchical structure.
        
        Returns:
            dict: A dictionary mapping comment IDs to their data and hierarchy info
        """
        # Extract comment areas 
        comment_areas = response.css('div.commentarea')
        if not comment_areas:
            self.logger.warning("No comment area found on the page")
            return {}
        
        # Create a mapping of comment IDs to their data and parent info
        comment_hierarchy = {}
        
        # First, extract top-level comments
        top_level_comments = comment_areas.css('div.sitetable.nestedlisting > div.thing.comment')
        self.logger.info(f"Found {len(top_level_comments)} top-level comments")
        
        # Process each top-level comment
        for comment in top_level_comments:
            comment_id = comment.css('::attr(data-fullname)').get()
            if not comment_id:
                continue
            
            # Extract comment data
            comment_data = self.extract_comment_data(comment)
            
            # Add to hierarchy with top-level flag
            comment_hierarchy[comment_id] = {
                'comment_obj': comment_data,
                'parent_id': None,  # Top-level comments have the post as parent
                'is_top_level': True,
                'replies': []
            }
            
            # Find child comments container
            child_div = comment.css('div.child')
            if child_div:
                # Process child comments (replies)
                self.process_nested_comments(child_div, comment_id, comment_hierarchy)
        
        return comment_hierarchy
    
    def process_nested_comments(self, child_div, parent_comment_id, comment_hierarchy):
        """
        Process nested comments (replies) recursively.
        
        Args:
            child_div: The div.child element containing replies
            parent_comment_id: ID of the parent comment
            comment_hierarchy: The mapping to update with comment data
        """
        # Find all direct child comments
        child_comments = child_div.css('div.sitetable > div.thing.comment')
        
        # If no comments found with CSS selector, try using BeautifulSoup for more flexible parsing
        if not child_comments:
            self.logger.debug(f"No child comments found with CSS selector, trying BeautifulSoup fallback")
            html = child_div.get()
            if html:
                try:
                    soup = BeautifulSoup(html, 'html.parser')
                    child_comments_soup = soup.select('div.sitetable > div.thing.comment')
                    
                    # If found with BeautifulSoup, convert to response elements
                    if child_comments_soup:
                        self.logger.debug(f"Found {len(child_comments_soup)} child comments with BeautifulSoup")
                        for child_soup in child_comments_soup:
                            # Extract comment data using our helper method
                            child_id = child_soup.get('data-fullname')
                            if not child_id:
                                continue
                                
                            # Create a simplified response-like object with basic data
                            comment_data = {
                                'id': child_id,
                                'author': self.extract_text_from_soup(child_soup, 'a.author'),
                                'body_text': self.encode_unicode(self.extract_text_from_soup(child_soup, 'div.md')),
                                'created': self.extract_attr_from_soup(child_soup, 'time', 'datetime'),
                                'permalink': self.extract_attr_from_soup(child_soup, 'a.bylink', 'href'),
                            }
                            
                            # Add to hierarchy
                            comment_hierarchy[child_id] = {
                                'comment_obj': comment_data,
                                'parent_id': parent_comment_id,
                                'is_top_level': False,
                                'replies': []
                            }
                            
                            # Add as reply to parent
                            comment_hierarchy[parent_comment_id]['replies'].append(child_id)
                            
                            # Process nested replies
                            child_div_soup = child_soup.select_one('div.child')
                            if child_div_soup:
                                self.process_nested_comments_soup(child_div_soup, child_id, comment_hierarchy)
                except Exception as e:
                    self.logger.error(f"Error parsing nested comments with BeautifulSoup: {e}")
        
        # Process with standard Scrapy selectors if found
        for child_comment in child_comments:
            child_id = child_comment.css('::attr(data-fullname)').get()
            if not child_id:
                continue
            
            # Extract child comment data
            child_data = self.extract_comment_data(child_comment)
            
            # Add to hierarchy with parent reference
            comment_hierarchy[child_id] = {
                'comment_obj': child_data,
                'parent_id': parent_comment_id,
                'is_top_level': False,
                'replies': []
            }
            
            # Add this comment as a reply to its parent
            comment_hierarchy[parent_comment_id]['replies'].append(child_id)
            
            # Process nested child comments recursively
            nested_child_div = child_comment.css('div.child')
            if nested_child_div:
                self.process_nested_comments(nested_child_div, child_id, comment_hierarchy)
    
    def process_nested_comments_soup(self, child_div_soup, parent_comment_id, comment_hierarchy):
        """
        Process nested comments using BeautifulSoup.
        
        Args:
            child_div_soup: BeautifulSoup object for div.child
            parent_comment_id: ID of parent comment
            comment_hierarchy: Comment hierarchy to update
        """
        try:
            # Find all comment elements
            child_comments = child_div_soup.select('div.sitetable > div.thing.comment')
            
            for child_comment in child_comments:
                child_id = child_comment.get('data-fullname')
                if not child_id:
                    continue
                    
                # Create a simplified comment object
                comment_data = {
                    'id': child_id,
                    'author': self.extract_text_from_soup(child_comment, 'a.author'),
                    'body_text': self.encode_unicode(self.extract_text_from_soup(child_comment, 'div.md')),
                    'created': self.extract_attr_from_soup(child_comment, 'time', 'datetime'),
                }
                
                # Add to hierarchy
                comment_hierarchy[child_id] = {
                    'comment_obj': comment_data,
                    'parent_id': parent_comment_id,
                    'is_top_level': False,
                    'replies': []
                }
                
                # Add as reply to parent
                comment_hierarchy[parent_comment_id]['replies'].append(child_id)
                
                # Process nested replies
                nested_child_div = child_comment.select_one('div.child')
                if nested_child_div:
                    self.process_nested_comments_soup(nested_child_div, child_id, comment_hierarchy)
        except Exception as e:
            self.logger.error(f"Error in process_nested_comments_soup: {e}")
    
    ############################
    # BeautifulSoup Helpers #
    ############################
    
    def extract_text_from_soup(self, element, selector):
        """Helper method to extract text from BeautifulSoup element."""
        try:
            selected = element.select_one(selector)
            return selected.get_text(strip=True) if selected else ""
        except Exception:
            return ""
        
    def extract_attr_from_soup(self, element, selector, attr):
        """Helper method to extract attribute from BeautifulSoup element."""
        try:
            selected = element.select_one(selector)
            return selected.get(attr) if selected else None
        except Exception:
            return None
    
    #############################
    # Comment Tree Building #
    #############################
    
    def build_replies_tree(self, reply_ids, comment_hierarchy, post_id):
        """
        Recursively build the replies tree for a comment.
        
        Args:
            reply_ids (list): List of reply comment IDs
            comment_hierarchy (dict): The mapping of all comments
            post_id (str): The ID of the post
            
        Returns:
            list: A list of comment objects with their nested replies
        """
        replies = []
        
        for reply_id in reply_ids:
            if reply_id in comment_hierarchy:
                reply_data = comment_hierarchy[reply_id]
                reply_obj = reply_data['comment_obj']
                
                # Add post_id to the reply
                reply_obj['post_id'] = post_id
                
                # Recursively build replies to this reply
                if reply_data['replies']:
                    nested_replies = self.build_replies_tree(
                        reply_data['replies'],
                        comment_hierarchy,
                        post_id
                    )
                    reply_obj['replies'] = nested_replies
                else:
                    reply_obj['replies'] = []
                    
                replies.append(reply_obj)
        
        return replies
    
    def extract_comment_data(self, comment):
        """Extract data from a comment element."""
        comment_id = comment.css('::attr(data-fullname)').get()
        author = comment.css('a.author::text').get()
        created = comment.css('time::attr(datetime)').get()
        
        # Extract permalink
        permalink = comment.css('a.bylink::attr(href)').get()
        
        # Extract parent link if available (for non-top-level comments)
        parent_link = comment.css('a[data-event-action="parent"]::attr(href)').get()
        
        # Extract all text from the comment, including text within links and other HTML elements
        # Instead of just using ::text which only gets direct text nodes, get all text content
        body_html = comment.css('div.md').get()  # Get the HTML content
        body_text = ""
        if body_html:
            # Parse the HTML and extract all text
            soup = BeautifulSoup(body_html, 'html.parser')
            body_text = soup.get_text(separator=' ', strip=True)
        
        # Extract score - old.reddit format has score spans with titles
        score_dislikes = comment.css('span.score.dislikes::attr(title)').get()
        score_unvoted = comment.css('span.score.unvoted::attr(title)').get() 
        score_likes = comment.css('span.score.likes::attr(title)').get()
        
        # If no score in title attributes, try to get text content
        if not score_unvoted:
            score_text = comment.css('span.score.unvoted::text').get()
            if score_text:
                # Extract number from text like "5 points"
                score_match = re.search(r'(\d+)', score_text)
                if score_match:
                    score_unvoted = score_match.group(1)
        
        # Extract additional metadata
        num_children_text = comment.css('a.numchildren::text').get()
        num_children = None
        if num_children_text:
            # Extract number from text like "(3 children)"
            children_match = re.search(r'\((\d+)\s+child', num_children_text)
            if children_match:
                num_children = int(children_match.group(1))
        
        # Extract created_utc if available
        created_utc = None
        if created:
            try:
                # Convert ISO format to UTC timestamp
                dt = datetime.datetime.fromisoformat(created.replace('Z', '+00:00'))
                created_utc = int(dt.timestamp())
            except Exception as e:
                self.logger.error(f"Error parsing comment timestamp: {e}")
        
        return {
            'id': comment_id,
            'author': author,
            'created': created,
            'created_utc': created_utc,
            'body_text': self.encode_unicode(body_text),
            'score_dislikes': score_dislikes,
            'score_unvoted': score_unvoted,
            'score_likes': score_likes,
            'num_children': num_children
        }
        
    ######################
    # Spider Lifecycle #
    ######################
    
    def close(self, reason):
        """Called when the spider closes for any reason."""
        self.save_processed_urls()