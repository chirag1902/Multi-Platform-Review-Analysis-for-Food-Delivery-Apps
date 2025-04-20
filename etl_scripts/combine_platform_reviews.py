import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

def setup_logging(base_dir):
    """Set up logging configuration."""
    logs_dir = base_dir / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = logs_dir / f'combined_platform_review.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging initialized. Log file: {log_file}")

# Define final column order
FINAL_COLUMNS = ['review', 'review_datetime', 'data_source', 'app_name',
                 'upvote_count', 'total_comments', 'app_rating']

def load_and_standardize(file_path, source_name):
    """Load a CSV file and standardize its columns to match the final format."""
    try:
        if not file_path.exists():
            logging.warning(f"File not found: {file_path}")
            return pd.DataFrame(columns=FINAL_COLUMNS)
        
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} records from {file_path}")
        
        # Add missing columns with None
        for col in FINAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
                logging.info(f"Added missing column '{col}' to {source_name} data")
        
        # Reorder columns
        df = df[FINAL_COLUMNS]
        return df
    
    except Exception as e:
        logging.error(f"Error loading {file_path}: {str(e)}")
        return pd.DataFrame(columns=FINAL_COLUMNS)

def combine_reviews_for_platform(base_dir, app_path):
    """Combine reviews from multiple sources for a single platform."""
    try:
        processed_dir = base_dir / 'data' / app_path / 'processed_data'
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        logging.info(f"Processing platform: {app_path}")
        
        # Define source file paths
        reddit_path = processed_dir / 'reddit.csv'
        google_path = processed_dir / 'google_play.csv'
        app_store_path = processed_dir / 'app_store.csv'

        # Load and standardize data
        reddit_df = load_and_standardize(reddit_path, 'reddit')
        google_df = load_and_standardize(google_path, 'google_play')
        app_store_df = load_and_standardize(app_store_path, 'app_store')

        combined_df = pd.concat([reddit_df, google_df, app_store_df], ignore_index=True)
        
        # Check if we have any data
        if combined_df.empty:
            logging.warning(f"No data found for {app_path}")
            return False

        # Create stats for logging
        stats = {
            'Total reviews': len(combined_df),
            'Reddit reviews': len(reddit_df),
            'Google Play reviews': len(google_df),
            'App Store reviews': len(app_store_df)
        }
        logging.info(f"Statistics for {app_path}: {stats}")

        output_path = processed_dir / 'combined_reviews.csv'
        combined_df.to_csv(output_path, index=False)
        
        logging.info(f"SUCCESS: Combined reviews saved for {app_path} at {output_path}")
        return True
        
    except Exception as e:
        logging.error(f"Error processing platform {app_path}: {str(e)}")
        return False

def main(app_path):
    """
    Main function to combine reviews for a specific platform.
    
    Args:
        app_path (str): The name of the platform to process (e.g., 'uber_eats').
    
    Returns:
        bool: True if processing was successful, False otherwise.
    """
    try:
        # Get the base directory (two levels up from the script location)
        base_dir = Path(__file__).parent.parent
        
        # Set up logging
        setup_logging(base_dir)
        logging.info(f"Starting review combination process for {app_path} from base directory: {base_dir}")
        
        # Process the specified platform
        return combine_reviews_for_platform(base_dir, app_path)
    except Exception as e:
        print(f"Critical error in main function: {str(e)}")
        logging.critical(f"Critical error in main function: {str(e)}")
        return False

