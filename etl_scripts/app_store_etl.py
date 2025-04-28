import pandas as pd
from datetime import date
from app_store_scraper import AppStore
import os
import logging
import sqlite3
from pathlib import Path

# -------------------------------
# LOGGING SETUP
# -------------------------------
logs_dir = Path(__file__).parent.parent / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)  # Create 'logs' directory if it doesn't exist

# Define the log file path
log_file_path = logs_dir / 'app_store_etl.log'

logging.basicConfig(
    filename=str(log_file_path),
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# ------------------------------------
# RAW DATA FUNCTION
# ------------------------------------

def Raw_Reviews(app_name, app_id, country='us', n=10000):
    logger.info(f"🔄 Starting raw review fetch for {app_name}")
    try:
        app = AppStore(country=country, app_name=app_name, app_id=app_id)
        app.review(how_many=n)
        df = pd.DataFrame(app.reviews)

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            today = pd.to_datetime(date.today())
            last_year = today.replace(year=today.year - 1)
            df = df[(df['date'] >= last_year) & (df['date'] <= today)]
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)

        return df
    except Exception as e:
        logger.error(f"❌ Error in Raw_Reviews for {app_name}: {str(e)}")
        return pd.DataFrame()

# ------------------------------------
# SAVE RAW DATA FUNCTION
# ------------------------------------

def save_raw_data(df, app_path):
    """Save the raw data to the raw data folder in multiple formats (CSV, Excel, SQLite)"""
    # Create directory structure if it doesn't exist using Path
    data_dir = Path(__file__).parent.parent / 'data' / app_path / 'raw_data'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Save to CSV
    csv_file = data_dir / 'app_store.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"Raw data saved to CSV: {csv_file}")
    
    # 2. Save to Excel
    excel_file = data_dir / 'app_store.xlsx'
    df.to_excel(excel_file, index=False)
    logger.info(f"Raw data saved to Excel: {excel_file}")
    
    # 3. Save to SQLite
    sqlite_file = data_dir / 'app_store.db'
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

# ------------------------------------
# PROCESSED DATA FUNCTION
# ------------------------------------

def Processed_Reviews(df_raw: pd.DataFrame, app_name: str):
    logger.info(f"✨ Starting review processing for {app_name}")
    try:
        if df_raw.empty or "review" not in df_raw.columns:
            logger.warning(f"⚠️ No valid data to process for {app_name}")
            return pd.DataFrame()

        df_clean = df_raw.rename(columns={"date": "review_datetime"})
        df_clean = df_clean[["review_datetime", "review", "rating"]].copy()
        df_clean.dropna(subset=["review_datetime", "review"], inplace=True)

        df_clean["data_source"] = "App Store"
        df_clean["app_name"] = app_name

        return df_clean
    except Exception as e:
        logger.error(f" Error in Processed_Reviews for {app_name}: {str(e)}")
        return pd.DataFrame()

# ------------------------------------
# SAVE PROCESSED DATA FUNCTION
# ------------------------------------

def save_processed_data(df, app_path):
    """Save the processed data to the processed data folder in multiple formats (CSV, Excel, SQLite)"""
    # Create directory structure if it doesn't exist using Path
    data_dir = Path(__file__).parent.parent / 'data' / app_path / 'processed_data'
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Save to CSV
    csv_file = data_dir / 'app_store.csv'
    df.to_csv(csv_file, index=False)
    logger.info(f"Processed data saved to CSV: {csv_file}")
    
    # 2. Save to Excel
    excel_file = data_dir / 'app_store.xlsx'
    df.to_excel(excel_file, index=False)
    logger.info(f"Processed data saved to Excel: {excel_file}")
    
    # 3. Save to SQLite
    sqlite_file = data_dir / 'app_store.db'
    conn = sqlite3.connect(sqlite_file)
    
    # Convert datetime columns to string to avoid SQLite datetime issues
    df_sqlite = df.copy()
    for col in df_sqlite.columns:
        if pd.api.types.is_datetime64_any_dtype(df_sqlite[col]):
            df_sqlite[col] = df_sqlite[col].astype(str)
    
    try:
        # Create a table 'processed_data' and save the dataframe
        df_sqlite.to_sql('processed_data', conn, if_exists='replace', index=False)
        logger.info(f"Processed data saved to SQLite database: {sqlite_file}")
    except Exception as e:
        logger.error(f"Error saving processed data to SQLite: {e}")
    finally:
        conn.close()
    
    return str(csv_file)

# ------------------------------------
# SANITY CHECKS FUNCTION
# ------------------------------------
def sanity_checks(df: pd.DataFrame, context: str = ""):
    logger.info(f"🧪 Running sanity checks {f'for {context}' if context else ''}")

    # Check if dataframe is empty
    assert not df.empty, f"Sanity Check Failed: {context} - DataFrame is empty!"

    # Check for missing values
    assert df.isnull().sum().sum() == 0, f"Sanity Check Failed: {context} - Missing values detected!"

    # Check if number of rows is reasonable
    min_expected_rows = 5  # Adjust based on expectations
    assert len(df) >= min_expected_rows, f"Sanity Check Failed: {context} - Unexpectedly low number of rows: {len(df)}"
    
    logger.info(f"✅ Sanity checks passed {f'for {context}' if context else ''}")


# ------------------------------------
# MAIN WRAPPER FUNCTION
# ------------------------------------

def main(app_name: str, app_path: str, app_id: int, review_count: int = 1000, country: str = 'us'):
    logger.info(f"🚀 Starting ETL pipeline for {app_name}")

    # Run raw ETL
    df_raw = Raw_Reviews(app_name, app_id, country, n=review_count)
    
    # --- SANITY CHECK raw data ---
    sanity_checks(df_raw, context="Raw Data")

    # --- BASIC PYTHON ASSERTS for raw data ---
    assert not df_raw.empty, " Raw DataFrame is empty!"
    assert df_raw.isnull().sum().sum() == 0, " Raw DataFrame has missing values!"
    assert len(df_raw) >= 5, f" Raw DataFrame has too few rows: {len(df_raw)}"

    # Save raw data in multiple formats
    save_raw_data(df_raw, app_path)
    
    # Process the raw data
    df_processed = Processed_Reviews(df_raw, app_name)
    
    # --- SANITY CHECK processed data ---
    sanity_checks(df_processed, context="Processed Data")

    # --- BASIC PYTHON ASSERTS for processed data ---
    assert not df_processed.empty, " Processed DataFrame is empty!"
    assert df_processed.isnull().sum().sum() == 0, " Processed DataFrame has missing values!"
    assert len(df_processed) >= 5, f" Processed DataFrame has too few rows: {len(df_processed)}"

    # Save processed data in multiple formats
    save_processed_data(df_processed, app_path)

    logger.info(f"Finished ETL pipeline for {app_name}")

    


