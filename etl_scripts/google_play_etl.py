# etl_scripts/google_play_etl.py
import os
import pandas as pd
from google_play_scraper import Sort, reviews_all
from datetime import datetime
import logging
from datetime import datetime, timedelta
import random
import sqlite3


try:
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # one level up from etl_scripts
except NameError:
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))  # for notebooks or interactive

logs_dir = os.path.join(BASE_DIR, "logs")
log_file_path = os.path.join(logs_dir, "etl_googleplay.log")


# Ensure logs directory exists
os.makedirs(logs_dir, exist_ok=True)

# Ensure the .log file exists
if not os.path.exists(log_file_path):
    with open(log_file_path, 'w') as f:
        f.write("")

# Remove all existing handlers before reconfiguring
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure logging (now it will definitely work)
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Test log
logging.info("test log message")


###### EXTRACTION ########

def extract_reviews(app_id: str, lang: str = "en", country: str = "us") -> pd.DataFrame:
    
    logging.info(f"Extracting reviews for {app_id}")
    #print(f"Extracting reviews for {app_id}")
    reviews = reviews_all(
        app_id,
        sleep_milliseconds=0,
        lang=lang,
        country=country,
        sort=Sort.NEWEST,
    )
    df = pd.DataFrame(reviews)

    logging.info(f"Extracted {len(df)} reviews for {app_id}")
    #print(f"Extracted {len(df)} reviews for {app_id}")

    # Assuming df is your DataFrame
    df['at'] = pd.to_datetime(df['at'])
    
    # Calculate one year ago from the current date/time
    one_year_ago = datetime.now() - timedelta(days=365)
    
    # Filter for reviews from the past year
    df = df[df['at'] >= one_year_ago]
    
    # Optional: Reset index
    df = df.reset_index(drop=True)
    
    logging.info(f"Extracted {len(df)} reviews for {app_id} in last 1 year ({one_year_ago})")
    #print(f"Extracted {len(df)} reviews for {app_id} in last 1 year ({one_year_ago})")
    
    return df


######## TRANSFORMATION ############

def transform_reviews(df: pd.DataFrame, app_name:str) -> pd.DataFrame:
    logging.info("Transforming reviews...")
    df = df.rename(columns={
        'content': 'review',
        'score': 'app_rating',
        'thumbsUpCount': 'upvote_count',
        'at': 'review_datetime',
    })

    ## Handle null/missing values
    df['review_datetime'] = pd.to_datetime(df['review_datetime'], errors='coerce')
    df = df[df['review_datetime'].notnull()]
    
    # Drop rows with missing reviews (content is empty)
    df = df[df['review'].notnull()]
    
    df['upvote_count'] = df['upvote_count'].fillna(0)
    
    ## rearrange the df
    df = df[['review', 'review_datetime',  'upvote_count', 'app_rating']]
    df['data_source'] = "Google Play"
    df['app_name'] = app_name

    logging.info("Transformation complete.")
    #print("Transformation complete")
    return df 


############ LOAD ##################


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def load_reviews(df: pd.DataFrame, app_path: str, path: str):
    # Define base directory relative to parent (../data/...)
    base_dir = os.path.join(BASE_DIR, "data", app_path.lower(), path)
    os.makedirs(base_dir, exist_ok=True)

    # Save as CSV
    csv_path = os.path.join(base_dir, "google_play.csv")
    df.to_csv(csv_path, index=False)
    logging.info(f"Saved {path} reviews to CSV: {csv_path}")

    # Save as Excel
    excel_path = os.path.join(base_dir, "google_play.xlsx")
    df.to_excel(excel_path, index=False)
    logging.info(f"Saved {path} reviews to Excel: {excel_path}")

    # Save to SQLite
    db_path = os.path.join(base_dir, "google_play.db")
    conn = sqlite3.connect(db_path)

    df_sqlite = df.copy()
    if 'review_datetime' in df_sqlite.columns:
        df_sqlite['review_datetime'] = df_sqlite['review_datetime'].astype(str)

    try:
        df_sqlite.to_sql(path, conn, if_exists='replace', index=False)
        logging.info(f"{path} saved to SQLite database: {db_path}")
    except Exception as e:
        logging.error(f"Error saving {path} to SQLite: {e}")
    finally:
        conn.close()

    logging.info("===== DATA LOADING COMPLETE =====")
    logging.info(f"Saved {len(df)} records in CSV, Excel and SQLite formats.")




########## MAIN ##########

def main(app_id: str, app_path: str, app_name: str):
    try:
        logging.info(f"ETL started for {app_name}")

        df_raw = extract_reviews(app_id)
        logging.info(f"Raw {app_name} data extracted")

        load_reviews(df_raw, app_path, "raw_data")
        logging.info(f"Raw {app_name} files placed")

        df_clean = transform_reviews(df_raw, app_name)
        logging.info("Data cleaned and transformed")

        load_reviews(df_clean, app_path, "processed_data")
        logging.info(f"Processed {app_name} files placed")

        logging.info(f"ETL completed for {app_name}")

    except Exception as e:
        logging.error(f"Error processing {app_name}: {e}")


