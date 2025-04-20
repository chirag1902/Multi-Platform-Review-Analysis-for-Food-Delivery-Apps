import os
import time
import asyncio
import asyncpraw
import pandas as pd
import logging
import yaml
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Create directory for logs if it doesn't exist
# Define the logs directory path relative to this script
logs_dir = Path(__file__).parent.parent / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)  # Create 'logs' directory if it doesn't exist

# Define the log file path
log_file_path = logs_dir / 'reddit_etl.log'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='a'),  # Append mode
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
#############################################
# CONFIGURATION LOADING
#############################################

def load_config():
    """Load configuration from config.yaml file"""
    try:
        # Navigate one level up from the current script, then into 'config'
        config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'

        # Optional: Debugging info
        print(f"Looking for config at: {config_path}")
        print("File exists?", config_path.exists())

        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

            reddit_config = config.get('reddit_api', {})
            for key, value in reddit_config.items():
                if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                    env_var = value[2:-1]
                    reddit_config[key] = os.environ.get(env_var)

                    if reddit_config[key] is None:
                        raise ValueError(f"Environment variable {env_var} not found")

            return config

    except Exception as e:
        raise e
#############################################
# EXTRACTION PART
#############################################

async def main(app_name, subreddit_name, app_path):
    """Main function to orchestrate the asynchronous data collection process"""
    logger.info(f"Starting ETL process for app: {app_name}, subreddit: {subreddit_name}, path: {app_path}")
    
    # Load configuration
    config = load_config()
    reddit_config = config.get('reddit_api', {})
    
    # Initialize Reddit API client using asyncpraw with config values
    reddit = asyncpraw.Reddit(
        client_id=reddit_config.get('client_id'),
        client_secret=reddit_config.get('client_secret'),
        user_agent=reddit_config.get('user_agent')
    )

    # Set the subreddit to scrape using subreddit_name
    subreddit = await reddit.subreddit(subreddit_name)

    # Calculate date 1 year ago from today
    one_year_ago = int((datetime.now() - timedelta(days=365)).timestamp())
    start_date = datetime.fromtimestamp(one_year_ago).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Collecting posts from {start_date} to {end_date}")

    # Initialize tracking of processed IDs to avoid duplicates
    processed_ids = set()

    # Run monthly collection for better historical coverage
    monthly_data = await collect_monthly_paginated(reddit, subreddit, one_year_ago, processed_ids)
    logger.info(f"Collected {len(monthly_data)} items from monthly pagination")
    logger.info(f"Posts: {len([x for x in monthly_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in monthly_data if x['type'] == 'comment'])}")

    # Run each collection method separately
    new_data = await collect_sorting(reddit, subreddit, one_year_ago, processed_ids, 'new')
    logger.info(f"Collected {len(new_data)} items from 'new' sorting")
    logger.info(f"Posts: {len([x for x in new_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in new_data if x['type'] == 'comment'])}")

    hot_data = await collect_sorting(reddit, subreddit, one_year_ago, processed_ids, 'hot')
    logger.info(f"Collected {len(hot_data)} items from 'hot' sorting")
    logger.info(f"Posts: {len([x for x in hot_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in hot_data if x['type'] == 'comment'])}")

    top_data = await collect_sorting(reddit, subreddit, one_year_ago, processed_ids, 'top')
    logger.info(f"Collected {len(top_data)} items from 'top' sorting")
    logger.info(f"Posts: {len([x for x in top_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in top_data if x['type'] == 'comment'])}")

    controversial_data = await collect_sorting(reddit, subreddit, one_year_ago, processed_ids, 'controversial')
    logger.info(f"Collected {len(controversial_data)} items from 'controversial' sorting")
    logger.info(f"Posts: {len([x for x in controversial_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in controversial_data if x['type'] == 'comment'])}")

    search_data = await collect_search_posts(reddit, subreddit, one_year_ago, processed_ids)
    logger.info(f"Collected {len(search_data)} items from keyword searches")
    logger.info(f"Posts: {len([x for x in search_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in search_data if x['type'] == 'comment'])}")

    flair_data = await collect_flair_posts(reddit, subreddit, one_year_ago, processed_ids)
    logger.info(f"Collected {len(flair_data)} items from flair searches")
    logger.info(f"Posts: {len([x for x in flair_data if x['type'] == 'post'])}")
    logger.info(f"Comments: {len([x for x in flair_data if x['type'] == 'comment'])}")

    # Combine all data
    all_data = monthly_data + new_data + hot_data + top_data + controversial_data + search_data + flair_data

    # Process data
    df = process_dataframe(all_data)
    
    # Save raw data
    save_raw_data(df, app_path)

    # Apply transformations
    df_transformed = transform_data(df, app_name)

    # Load the processed data
    load_data(df_transformed, app_path)

    # Run sanity tests
    run_sanity_tests(df_transformed)

    # Close the Reddit instance
    await reddit.close()
    
    logger.info(f"ETL process completed for {app_name}")

async def collect_monthly_paginated(reddit, subreddit, start_timestamp, processed_ids):
    """Extracts posts using monthly time windows to avoid Reddit API limits."""
    data = []
    end_timestamp = int(datetime.now().timestamp())

    while start_timestamp < end_timestamp:
        month_end = min(start_timestamp + (30 * 24 * 60 * 60), end_timestamp)

        month_start_date = datetime.fromtimestamp(start_timestamp).strftime('%Y-%m-%d')
        month_end_date = datetime.fromtimestamp(month_end).strftime('%Y-%m-%d')
        logger.info(f"Collecting posts from {month_start_date} to {month_end_date}")

        query = f"timestamp:{start_timestamp}..{month_end}"

        try:
            posts_collected = 0
            async for submission in subreddit.search(query, sort="new", limit=None):
                # Verify post is within our time window
                if submission.created_utc < start_timestamp or submission.created_utc > month_end:
                    continue

                if submission.id in processed_ids:
                    continue

                # Extract submission data
                post_data = await extract_submission_data(submission)
                if post_data:
                    post_data['source'] = f"monthly_{month_start_date}"
                    data.append(post_data)
                    processed_ids.add(submission.id)

                    # Process comments for this submission
                    comment_data = await process_comments(submission)
                    for item in comment_data:
                        if item['id'] not in processed_ids:
                            data.append(item)
                            processed_ids.add(item['id'])

                posts_collected += 1
                if posts_collected % 25 == 0:
                    logger.info(f"Collected {posts_collected} posts from time window {month_start_date}")

                # Small delay to avoid rate limits
                await asyncio.sleep(0.05)

        except Exception as e:
            logger.error(f"Error collecting posts for time window {month_start_date}: {e}")

        # Move to next month
        start_timestamp = month_end

    logger.info(f"Finished monthly pagination collection. Total items: {len(data)}")
    return data

async def collect_sorting(reddit, subreddit, since, processed_ids, method):
    """Extracts posts using a specified subreddit sorting method (new, hot, top, controversial)."""
    data = []
    logger.info(f"Collecting posts from '{method}' sorting...")

    try:
        fetch = getattr(subreddit, method)

        # Handle time filters for certain sorting methods
        time_filters = ['all', 'year', 'month', 'week'] if method in ['top', 'controversial'] else [None]

        for time_filter in time_filters:
            posts_collected = 0

            if time_filter:
                logger.info(f"Using time filter '{time_filter}' with {method} sorting")
                sorting_iterator = fetch(limit=None, time_filter=time_filter)
            else:
                sorting_iterator = fetch(limit=None)

            async for submission in sorting_iterator:
                # Skip if post is older than cutoff date
                if submission.created_utc < since:
                    # For 'new' sorting, we can break early as posts are chronological
                    if method == 'new':
                        logger.info(f"Reached posts older than cutoff in '{method}' sorting")
                        break
                    continue

                if submission.id in processed_ids:
                    continue

                # Extract submission data
                post_data = await extract_submission_data(submission)
                if post_data:
                    source_name = method if time_filter is None else f"{method}_{time_filter}"
                    post_data['source'] = source_name
                    data.append(post_data)
                    processed_ids.add(submission.id)

                    # Process comments for this submission
                    comment_data = await process_comments(submission)
                    for item in comment_data:
                        if item['id'] not in processed_ids:
                            data.append(item)
                            processed_ids.add(item['id'])

                posts_collected += 1
                if posts_collected % 50 == 0:
                    logger.info(f"Collected {posts_collected} posts from '{method}' sorting")

                # Small delay to avoid rate limits
                await asyncio.sleep(0.05)

            if time_filter:
                logger.info(f"Finished collecting {posts_collected} posts from '{method}' with filter '{time_filter}'")

    except Exception as e:
        logger.error(f"Error collecting posts from '{method}' sorting: {e}")

    logger.info(f"Finished collecting from '{method}' sorting. Total items: {len(data)}")
    return data

async def collect_search_posts(reddit, subreddit, since, processed_ids):
    """Extracts posts by searching for defined keywords related to user experience."""
    data = []

    # Search terms relevant to food delivery customer experience
    search_terms = [
        # Core search terms
        "problem", "issue", "terrible", "bad", "great", "awesome",
        "driver", "delivery", "order", "customer service", "refund",
        "cancel", "late", "missing", "wrong", "charge", "tip",
        "food", "cold", "wait", "app", "error", "crash", "glitch",
        "overcharge", "price", "expensive", "discount", "promo",

        # Additional search terms
        "experience", "rating", "dasher", "courier", "vehicle", "bike", "car",
        "delayed", "damaged", "quality", "packaging", "restaurant", "merchant",
        "address", "location", "gps", "map", "directions", "instructions",
        "communication", "contact", "support", "chat", "phone", "call", "text",
        "payment", "card", "wallet", "subscription", "plus", "fees", "tax",
        "receipt", "estimate", "actual", "notification", "tracking", "map",
        "account", "login", "password", "verification", "bonus", "reward",
        "satisfaction", "disappointed", "happy", "frustrated", "angry", "love",
        "hate", "never again", "first time", "loyal", "regular", "customer",
        "lost", "stolen", "compensation", "resolution", "solution", "fixed",
        "fast", "slow", "quick", "temperature", "hot", "warm", "frozen",
        "delivered", "received", "picked up", "preparing", "ready"
    ]

    # Add compound search terms
    compound_terms = [
        '"customer service"', '"delivery time"', '"wrong order"',
        '"missing items"', '"cold food"', '"wrong address"',
        '"not delivered"', '"damaged food"', '"bad experience"',
        '"great service"', '"friendly driver"', '"app issues"'
    ]
    search_terms.extend(compound_terms)

    for term in search_terms:
        logger.info(f"Searching for term: '{term}'...")
        try:
            posts_collected = 0
            async for submission in subreddit.search(term, sort="new", time_filter="year", limit=None):
                # Skip if post is older than cutoff
                if submission.created_utc < since:
                    continue

                if submission.id in processed_ids:
                    continue

                # Extract submission data
                post_data = await extract_submission_data(submission)
                if post_data:
                    post_data['source'] = f"search_{term}"
                    data.append(post_data)
                    processed_ids.add(submission.id)

                    # Process comments for this submission
                    comment_data = await process_comments(submission)
                    for item in comment_data:
                        if item['id'] not in processed_ids:
                            data.append(item)
                            processed_ids.add(item['id'])

                posts_collected += 1
                if posts_collected % 25 == 0:
                    logger.info(f"Collected {posts_collected} posts from search for '{term}'")

                # Small delay to avoid hitting rate limits
                await asyncio.sleep(0.05)

        except Exception as e:
            logger.error(f"Error searching for term '{term}': {e}")

        logger.info(f"Collected {posts_collected} posts from search term '{term}'")

    logger.info(f"Finished collecting from keyword searches. Total items: {len(data)}")
    return data

async def collect_flair_posts(reddit, subreddit, since, processed_ids):
    """Extracts posts based on specific post flairs in the subreddit."""
    data = []

    # Common flairs in food delivery subreddits
    flairs = ["Complaint", "Issue", "Question", "Discussion", "Experience",
              "Feedback", "Problem", "Help", "Rant", "PSA", "Warning", "Tip"]

    for flair in flairs:
        logger.info(f"Searching for posts with flair: '{flair}'...")
        search_query = f"flair:{flair}"

        try:
            posts_collected = 0
            async for submission in subreddit.search(search_query, sort="new", time_filter="year", limit=None):
                # Skip if post is older than cutoff
                if submission.created_utc < since:
                    continue

                if submission.id in processed_ids:
                    continue

                # Extract submission data
                post_data = await extract_submission_data(submission)
                if post_data:
                    post_data['source'] = f"flair_{flair}"
                    data.append(post_data)
                    processed_ids.add(submission.id)

                    # Process comments for this submission
                    comment_data = await process_comments(submission)
                    for item in comment_data:
                        if item['id'] not in processed_ids:
                            data.append(item)
                            processed_ids.add(item['id'])

                posts_collected += 1
                if posts_collected % 25 == 0:
                    logger.info(f"Collected {posts_collected} posts with flair '{flair}'")

                await asyncio.sleep(0.05)

        except Exception as e:
            logger.error(f"Error searching for flair '{flair}': {e}")

    logger.info(f"Finished collecting by flair. Total items: {len(data)}")
    return data

async def process_comments(submission):
    """Process all comments for a submission using asyncpraw"""
    comment_data = []

    try:
        # Fixed: Handle potential None comments before trying to access attributes
        if not hasattr(submission, 'comments') or submission.comments is None:
            logger.warning(f"Submission {submission.id} has no comments attribute or it's None")
            return comment_data

        # Extend comment limit and get all comments
        try:
            await submission.comments.replace_more(limit=None)
            all_comments = await submission.comments.list()
        except Exception as e:
            logger.warning(f"Couldn't fully load comments for submission {submission.id}: {e}")
            # Try to process any comments that were loaded
            if hasattr(submission, 'comments') and hasattr(submission.comments, '_comments'):
                all_comments = submission.comments._comments
            else:
                return comment_data  # Return empty list if no comments available

        for comment in all_comments:
            extracted_data = await extract_comment_data(comment, submission)
            if extracted_data:
                extracted_data['source'] = f"comment_in_{submission.id}"
                comment_data.append(extracted_data)
    except Exception as e:
        logger.warning(f"Error processing comments for submission {submission.id}: {e}")

    return comment_data

async def extract_submission_data(submission):
    """Extract data from a Reddit submission using asyncpraw"""
    try:
        # Convert timestamp to datetime for easier handling
        created_dt = datetime.fromtimestamp(submission.created_utc)

        # For self posts, get the text content
        selftext = ""
        if submission.is_self:
            selftext = submission.selftext

        # Combine title and selftext for content analysis
        full_text = f"{submission.title} {selftext}"

        # Skip if no meaningful text content
        if len(full_text.strip()) < 5:
            return None

        # Get author name safely
        author_name = '[deleted]'
        try:
            author_name = submission.author.name if submission.author else '[deleted]'
        except:
            pass

        return {
            'id': submission.id,
            'type': 'post',
            'complaint': full_text,
            'username': author_name,
            'timestamp': submission.created_utc,
            'timestamp_dt': created_dt,
            'date': created_dt.strftime('%Y-%m-%d'),
            'time': created_dt.strftime('%H:%M:%S'),
            'upvotes': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'total_comments': submission.num_comments,
            'url': submission.url,
            'permalink': f"https://www.reddit.com{submission.permalink}",
            'is_self_post': submission.is_self,
            'flair': submission.link_flair_text,
            'title': submission.title
        }
    except Exception as e:
        logger.error(f"Error processing submission {submission.id}: {e}")
        return None

async def extract_comment_data(comment, parent_submission):
    """Extract data from a Reddit comment using asyncpraw"""
    try:
        # Skip comments with no meaningful content
        if not hasattr(comment, 'body') or len(comment.body.strip()) < 5 or comment.body == '[deleted]' or comment.body == '[removed]':
            return None

        # Convert timestamp to datetime
        created_dt = datetime.fromtimestamp(comment.created_utc)

        # Get author name safely
        author_name = '[deleted]'
        try:
            author_name = comment.author.name if comment.author else '[deleted]'
        except:
            pass

        return {
            'id': comment.id,
            'type': 'comment',
            'complaint': comment.body,
            'username': author_name,
            'timestamp': comment.created_utc,
            'timestamp_dt': created_dt,
            'date': created_dt.strftime('%Y-%m-%d'),
            'time': created_dt.strftime('%H:%M:%S'),
            'upvotes': comment.score,
            'upvote_ratio': None,  # Comments don't have upvote ratio
            'total_comments': len(comment.replies) if hasattr(comment, 'replies') else 0,
            'url': None,
            'permalink': f"https://www.reddit.com{comment.permalink}" if hasattr(comment, 'permalink') else None,
            'is_self_post': None,
            'flair': None,
            'parent_submission_id': parent_submission.id,
            'parent_submission_title': parent_submission.title
        }
    except Exception as e:
        logger.error(f"Error processing comment {comment.id if hasattr(comment, 'id') else 'unknown'}: {e}")
        return None

def process_dataframe(data):
    """Process collected data into a DataFrame"""
    # Create DataFrame
    df = pd.DataFrame(data)

    # Remove any remaining duplicates based on id
    df = df.drop_duplicates(subset=['id'])

    # Sort by timestamp (newest first)
    df = df.sort_values(by='timestamp_dt', ascending=False)

    # Log summary statistics
    logger.info("===== DATA SUMMARY =====")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Posts: {len(df[df['type'] == 'post'])}")
    logger.info(f"Comments: {len(df[df['type'] == 'comment'])}")
    logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
    logger.info(f"Total unique users: {df['username'].nunique()}")
    
    # Log data sources
    source_counts = df['source'].value_counts().to_dict()
    logger.info(f"Data sources: {source_counts}")

    # Log sample of the data
    sample_data = df[['type', 'username', 'date', 'upvotes', 'total_comments']].head().to_string()
    logger.info(f"Sample data:\n{sample_data}")

    return df

def save_raw_data(df, app_path):
    """Save the raw data to the raw data folder in multiple formats (CSV, Excel, SQLite)"""
    # Create directory structure if it doesn't exist using Path
    data_dir = Path(__file__).parent.parent / 'data' / app_path / 'raw_data'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Save to CSV
    csv_file = data_dir / 'reddit.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"Raw data saved to CSV: {csv_file}")
    
    # 2. Save to Excel
    excel_file = data_dir / 'reddit.xlsx'
    df.to_excel(excel_file, index=False)
    logger.info(f"Raw data saved to Excel: {excel_file}")
    
    # 3. Save to SQLite
    sqlite_file = data_dir / 'reddit.db'
    conn = sqlite3.connect(sqlite_file)
    
    # Convert timestamp_dt to string to avoid SQLite datetime issues
    df_sqlite = df.copy()
    if 'timestamp_dt' in df_sqlite.columns:
        df_sqlite['timestamp_dt'] = df_sqlite['timestamp_dt'].astype(str)
    
    try:
        # Create a table 'raw_data' and save the dataframe
        df_sqlite.to_sql('raw_data', conn, if_exists='replace', index=False)
        logger.info(f"Raw data saved to SQLite database: {sqlite_file}")
    except Exception as e:
        logger.error(f"Error saving to SQLite: {e}")
    finally:
        conn.close()
    
    return str(csv_file)

#############################################
# TRANSFORMATION PART
#############################################

def transform_data(df, app_name):
    """Transform the raw data into the required format"""
    logger.info("Starting data transformation...")

    # Create a copy to avoid modifying the original
    df_transformed = df.copy()

    # Check for null values in critical columns before transformation
    logger.info("===== DATA QUALITY CHECK BEFORE TRANSFORMATION =====")
    logger.info(f"Null values in complaint: {df_transformed['complaint'].isnull().sum()}")
    logger.info(f"Null values in timestamp_dt: {df_transformed['timestamp_dt'].isnull().sum()}")
    logger.info(f"Zero upvotes count: {(df_transformed['upvotes'] == 0).sum()}")
    logger.info(f"Zero total comments: {(df_transformed['total_comments'] == 0).sum()}")

    # Remove rows with null values in critical columns
    initial_rows = len(df_transformed)
    df_transformed = df_transformed.dropna(subset=['complaint', 'timestamp_dt'])
    rows_after_null_removal = len(df_transformed)
    logger.info(f"Rows removed due to null values: {initial_rows - rows_after_null_removal}")

    # Convert all text in review column to lowercase
    df_transformed['complaint'] = df_transformed['complaint'].str.lower()
    logger.info("Converted all review text to lowercase")

    # Rename columns as requested
    df_transformed.rename(columns={
        'complaint': 'review',
        'timestamp_dt': 'review_datetime',
        'upvotes': 'upvote_count'
    }, inplace=True)

    # Add new columns
    df_transformed['data_source'] = 'Reddit'
    df_transformed['app_name'] = app_name  # Only place app_name is used

    # Drop unnecessary columns
    df_transformed.drop(columns=[
        'id', 'date', 'type', 'time', 'timestamp', 'url',
        'permalink', 'is_self_post', 'source', 'upvote_ratio',
        'flair', 'username', 'title'
    ], inplace=True)

    logger.info(f"Data transformation complete. Transformed data shape: {df_transformed.shape}")

    return df_transformed

#############################################
# LOADING PART
#############################################

def load_data(df, app_path):
    """Load the transformed data to multiple formats (CSV, Excel, SQLite) in the processed data folder"""
    logger.info("Starting data loading...")

    # Create directory structure if it doesn't exist using Path
    processed_dir = Path(__file__).parent.parent / 'data' / app_path / 'processed_data'
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Save to CSV
    csv_file = processed_dir / 'reddit.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"Processed data saved to CSV: {csv_file}")
    
    # 2. Save to Excel
    excel_file = processed_dir / 'reddit.xlsx'
    df.to_excel(excel_file, index=False)
    logger.info(f"Processed data saved to Excel: {excel_file}")
    
    # 3. Save to SQLite
    sqlite_file = processed_dir / 'reddit.db'
    conn = sqlite3.connect(sqlite_file)
    
    # Convert datetime to string to avoid SQLite datetime issues
    df_sqlite = df.copy()
    if 'review_datetime' in df_sqlite.columns:
        df_sqlite['review_datetime'] = df_sqlite['review_datetime'].astype(str)
    
    try:
        # Create a table 'processed_data' and save the dataframe
        df_sqlite.to_sql('processed_data', conn, if_exists='replace', index=False)
        logger.info(f"Processed data saved to SQLite database: {sqlite_file}")
    except Exception as e:
        logger.error(f"Error saving processed data to SQLite: {e}")
    finally:
        conn.close()

    logger.info("===== DATA LOADING COMPLETE =====")
    logger.info(f"Saved {len(df)} records in multiple formats")

    return str(csv_file)

#############################################
# SANITY TESTS
#############################################

def run_sanity_tests(df):
    """Run sanity tests on the transformed data"""
    logger.info("Running sanity tests...")

    logger.info("===== SANITY TESTS =====")

    # Test 1: Check for null values in critical columns
    null_counts = df.isnull().sum()
    logger.info(f"1. Null value counts:\n{null_counts}")

    assert df['review'].isnull().sum() == 0, "Error: Found null values in 'review' column"
    assert df['review_datetime'].isnull().sum() == 0, "Error: Found null values in 'review_datetime' column"

    # Test 2: Verify reasonable row count
    row_count = len(df)
    logger.info(f"2. Row count: {row_count}")
    assert row_count > 10, f"Error: DataFrame has only {row_count} rows, which seems too low"

    # Test 3: Verify all required columns exist
    required_columns = ['review', 'review_datetime', 'upvote_count', 'total_comments', 'data_source', 'app_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    logger.info(f"3. All required columns present: {len(missing_columns) == 0}")
    assert len(missing_columns) == 0, f"Error: Missing required columns: {missing_columns}"

    # Test 4: Check data types
    logger.info(f"4. Data types:\n{df.dtypes}")
    assert pd.api.types.is_datetime64_any_dtype(df['review_datetime']), "Error: review_datetime is not a datetime type"

    # Test 5: Check value distributions
    logger.info(f"5. Value distribution checks:")
    logger.info(f"   Data source unique values: {df['data_source'].unique()}")
    logger.info(f"   App name unique values: {df['app_name'].unique()}")

    # Test 6: Check review text quality
    short_reviews = (df['review'].str.len() < 5).sum()
    logger.info(f"6. Short reviews (less than 5 chars): {short_reviews}")
    assert short_reviews == 0, f"Error: Found {short_reviews} reviews that are too short"

    # Test 7: Check if all review text is lowercase
    uppercase_reviews = (df['review'].str.contains(r'[A-Z]')).sum()
    logger.info(f"7. Reviews with uppercase characters: {uppercase_reviews}")
    assert uppercase_reviews == 0, f"Error: Found {uppercase_reviews} reviews with uppercase characters"

    logger.info("All sanity tests passed!")
    
