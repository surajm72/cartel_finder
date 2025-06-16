"""
Run the data pipeline.
This script acts as an entry point for the pipeline, managing the flow of execution.
"""
import os
import sys
import logging
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("pipeline_runner")

# Create logs directory if it doesn't exist
Path("logs").mkdir(exist_ok=True)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the data pipeline.")
    
    parser.add_argument(
        "--collect",
        action="store_true",
        help="Run data collection step"
    )
    
    parser.add_argument(
        "--process",
        action="store_true",
        help="Run data processing step"
    )
    
    parser.add_argument(
        "--wait",
        type=int,
        default=5,
        help="Wait time between collection and processing (in seconds)"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all pipeline steps"
    )
    
    parser.add_argument(
        "--source",
        choices=["reddit", "news", "all"],
        default="all",
        help="Data source to process (reddit, news, or all)"
    )
    
    return parser.parse_args()


def run_command(command):
    """Run a command and return the success status."""
    logger.info(f"Running command: {command}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info(f"Command succeeded with output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with error: {e.stderr.strip()}")
        return False


def collect_reddit_data():
    """Collect Reddit data."""
    logger.info("Collecting Reddit data")
    
    # Check if reddit spider and run_scraper exist
    spider_path = "data_collection/reddit/spiders/reddit_spider.py"
    run_scraper_path = "data_collection/reddit/run_scraper.py"
    
    if os.path.exists(spider_path) and os.path.exists(run_scraper_path):
        logger.info("Reddit scraper found, running scraper...")
        return run_command("python data_collection/reddit/run_scraper.py")
    else:
        missing = []
        if not os.path.exists(spider_path):
            missing.append(spider_path)
        if not os.path.exists(run_scraper_path):
            missing.append(run_scraper_path)
        logger.warning(f"Reddit scraper files not found: {', '.join(missing)}")
        return False


def collect_news_data():
    """Collect News data."""
    logger.info("Collecting News data")
    
    # Check if news spiders and run_scraper exist
    spider_path = "data_collection/news/spiders/reuters_spider.py"
    run_scraper_path = "data_collection/news/run_scraper.py"
    
    if os.path.exists(spider_path) and os.path.exists(run_scraper_path):
        logger.info("News scraper found, running scraper...")
        return run_command("python data_collection/news/run_scraper.py")
    else:
        missing = []
        if not os.path.exists(spider_path):
            missing.append(spider_path)
        if not os.path.exists(run_scraper_path):
            missing.append(run_scraper_path)
        logger.warning(f"News scraper files not found: {', '.join(missing)}")
        return False


def process_reddit_data():
    """Process Reddit data."""
    logger.info("Processing Reddit data")
    return run_command("python data_pipeline/reddit_db_processor.py")

def process_news_data():
    """Process News data."""
    logger.info("Processing News data")
    return run_command("python data_pipeline/news_db_processor.py")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Default to all steps if none specified
    run_all = args.all or not (args.collect or args.process)
    
    # Determine which data sources to process
    process_reddit = args.source in ["reddit", "all"]
    process_news = args.source in ["news", "all"]
    
    # Run data collection
    if args.collect or run_all:
        collection_success = True
        
        # Reddit data collection
        if process_reddit:
            if not collect_reddit_data():
                logger.error("Reddit data collection failed")
                collection_success = False
        
        # News data collection
        if process_news:
            if not collect_news_data():
                logger.error("News data collection failed")
                collection_success = False
        
        # Exit if collection failed and we need to process the data
        if not collection_success and (args.process or run_all):
            logger.error("Data collection failed, skipping processing step")
            return 1
        
        # Wait between collection and processing
        if (args.process or run_all) and collection_success:
            logger.info(f"Waiting {args.wait} seconds before processing...")
            time.sleep(args.wait)
    
    # Run data processing
    if args.process or run_all:
        processing_success = True
        
        # Reddit data processing
        if process_reddit:
            if not process_reddit_data():
                logger.error("Reddit data processing failed")
                processing_success = False
        
        # News data processing (add when implemented)
        if process_news:
            if not process_news_data():
                logger.error("News data processing failed")
                processing_success = False
        
        if not processing_success:
            logger.error("Data processing failed")
            return 1
    
    logger.info("Pipeline execution completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main()) 