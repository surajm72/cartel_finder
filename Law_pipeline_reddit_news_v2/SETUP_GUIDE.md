# Setup Guide for Legal Data Pipeline

This document provides step-by-step instructions to set up and run the Legal Data Pipeline on a Windows system.

## 1. Prerequisites Installation

### 1.1 Install Python 3.8 or higher

1. Download Python from [python.org](https://www.python.org/downloads/windows/)
2. Run the installer and **check the box that says "Add Python to PATH"**
3. Click "Install Now" for a standard installation
4. Verify installation by opening Command Prompt and typing:
   ```
   python --version
   ```
   This should display the Python version.

### 1.2 Install PostgreSQL

1. Download PostgreSQL from [postgresql.org](https://www.postgresql.org/download/windows/)
2. Run the installer
3. When prompted:
   - Set password to `password` (or choose your own and update the config.ini file later)
   - Keep default port (5432)
   - Choose default locale
4. Complete the installation
5. The installer will ask if you want to launch Stack Builder - you can skip this step
6. Verify PostgreSQL is running by opening pgAdmin (installed with PostgreSQL)

### 1.3 Install Required Python Libraries

#### Option 1: Using the Installation Script (Recommended)

1. After installing Python, simply double-click the `install_dependencies.bat` file in the project folder
2. This will automatically install all required libraries

#### Option 2: Manual Installation

If the installation script doesn't work for any reason:

1. Open Command Prompt as Administrator
2. Upgrade pip:
   ```
   python -m pip install --upgrade pip
   ```
3. Navigate to the project directory:
   ```
   cd C:\LegalDataPipeline
   ```
4. Install all required libraries using the requirements.txt file:
   ```
   pip install -r requirements.txt
   ```

This will install all necessary dependencies including Scrapy, PostgreSQL connector, BeautifulSoup, and other required libraries.

## 2. Setup Project

### 2.1 Extract Project Files

1. Extract the provided ZIP file to a location of your choice (e.g., `C:\LegalDataPipeline`)
2. Navigate to the extracted folder in Command Prompt:
   ```
   cd C:\LegalDataPipeline
   ```

### 2.2 Configure Database Settings

If you used different database settings during PostgreSQL installation:

1. Open the `config.ini` file in the project root directory
2. Update the database section with your credentials:
   ```ini
   [database]
   host=localhost
   port=5432
   username=postgres
   password=YourPasswordHere
   database=legal_data
   ```

### 2.3 Configure Subreddits

To change which subreddits are scraped:

1. Open `config/reddit_config.py` in a text editor
2. Modify the `SUBREDDITS` list:
   ```python
   SUBREDDITS = [
       'legaladvice',
       'smallbusiness',
       'news',
       # Add or remove subreddits here
   ]
   ```

### 2.4 Configure Scraping Parameters

To adjust scraping behavior:

1. In the same `config/reddit_config.py` file, modify the `SCRAPING_CONFIG` dictionary:
   ```python
   SCRAPING_CONFIG = {
       # Number of posts to scrape per subreddit
       'posts_per_subreddit': 15,  # Increase/decrease as needed
       
       # Number of comments to scrape per post
       'comments_per_post': 25,
       
       # Delay between requests (in seconds)
       'request_delay': 2.0,  # Increase if getting rate limited
       
       # Sort method for posts
       'sort_method': 'new',  # Options: 'hot', 'new', 'top', 'rising'
   }
   ```

## 3. Running the Pipeline

### 3.1 Easy Run Using Batch Files

For convenience, batch files have been included that you can simply double-click to run:

1. `run_pipeline.bat` - Runs the complete pipeline (collection and processing)
2. `run_collection_only.bat` - Only collects data from Reddit
3. `run_processing_only.bat` - Only processes already collected data

Just double-click on these files in Windows Explorer to execute them.

### 3.2 Run Using Command Line

If you prefer to use the command line:

To run the complete pipeline:
```
python run_pipeline.py --all
```

To run only data collection:
```
python run_pipeline.py --collect
```

To run only data processing:
```
python run_pipeline.py --process
```

## 4. Accessing the Data

### 4.1 Using pgAdmin

1. Open pgAdmin (installed with PostgreSQL)
2. Connect to your database server
3. Navigate to Databases > legal_data > Schemas > public > Tables
4. You should see `reddit_posts` and `reddit_comments` tables
5. Right-click on a table and select "View/Edit Data" > "All Rows" to see the data

### 4.2 Using SQL

If you prefer direct SQL queries:

1. Open Command Prompt
2. Connect to PostgreSQL:
   ```
   psql -U postgres -d legal_data
   ```
3. Enter your password when prompted
4. Run SQL queries, for example:
   ```sql
   SELECT * FROM reddit_posts LIMIT 10;
   SELECT * FROM reddit_comments;
   SELECT * FROM reddit_comments WHERE post_id = 'some_post_id';
   ```

## 5. Troubleshooting

### 5.1 Database Connection Issues

If you encounter database connection problems:
- Ensure PostgreSQL service is running (check Windows Services)
- Verify credentials in `config.ini` match what you set during installation
- Try connecting using pgAdmin to confirm credentials are correct

### 5.2 Scraping Issues

If data collection fails:
- Increase `request_delay` in `config/reddit_config.py` to avoid rate limiting
- Check your internet connection
- Verify the subreddits in the configuration exist

### 5.3 Log Files

Check the log files for detailed error information:
- Logs are stored in the `logs` directory
- Pipeline execution logs: `pipeline_YYYYMMDD_HHMMSS.log`
- Reddit scraper logs: `reddit_YYYYMMDD_HHMMSS.log`
- Processing logs: `reddit_processor.log`