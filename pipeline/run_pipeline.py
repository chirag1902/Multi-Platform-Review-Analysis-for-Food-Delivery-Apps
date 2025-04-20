import yaml
import time
import asyncio
import logging
import pandas as pd
import sys
from pathlib import Path
from etl_scripts.app_store_etl import main as app_store_main
from etl_scripts.google_play_etl import main as play_store_main
from etl_scripts.reddit_etl import main as reddit_main
from etl_scripts.combine_platform_reviews import main as combine_main
from etl_scripts.s3_backup import main as s3_backup_main

# Setup paths
project_root = Path(__file__).parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)  # Create 'logs' directory if it doesn't exist
log_file_path = logs_dir / 'etl_process.log'

config_path = project_root / 'config' / 'config.yaml'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file_path)
    ]
)
logger = logging.getLogger('etl_controller')

def load_config():
    """Load configuration from config.yaml file"""
    try:
        # Optional: Debugging info
        print(f"Looking for config at: {config_path}")
        print("File exists?", config_path.exists())

        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        return None

async def process_app(app_config):
    """Process a single app across all platforms"""
    app_name = app_config['app_name']
    app_path = app_config['app_path']
    
    try:
        logger.info(f"Starting ETL process for {app_name}...")
        
        # Run App Store ETL
        logger.info(f"Running App Store ETL for {app_name}...")
        app_store_main(
            app_name=app_name,
            app_path=app_path,
            app_id=app_config['app_store_id'],
            country="us",
            review_count=20000
        )
        
        # Run Play Store ETL
        logger.info(f"Running Play Store ETL for {app_name}...")
        play_store_main(
            app_config['play_store_id'],
            app_path,
            app_name
        )
        
        # Run Reddit ETL
        logger.info(f"Running Reddit ETL for {app_name}...")
        await reddit_main(app_name, app_config['subreddit_name'], app_path)
        
        # Run Combine Platform Reviews
        logger.info(f"Combining platform reviews for {app_name}...")
        combine_success = combine_main(app_path)
        
        if combine_success:
            logger.info(f"Successfully combined reviews for {app_name}")
        else:
            logger.error(f"Failed to combine reviews for {app_name}")
            
        logger.info(f"Completed ETL process for {app_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {app_name}: {e}")
        logger.exception("Detailed traceback:")
        return False

def run_s3_backup():
    """Run the S3 backup process"""
    logger.info("Starting S3 backup process...")
    try:
        # Simply call the main function from the s3_backup module
        result = s3_backup_main()
        if result == 0:  # Success is indicated by a return code of 0
            logger.info("S3 backup completed successfully")
            return True
        else:
            logger.error(f"S3 backup failed with exit code: {result}")
            return False
    except Exception as e:
        logger.error(f"Error during S3 backup: {e}")
        logger.exception("Detailed traceback:")
        return False

def aggregate_review_data(app_configs):
    """Aggregate all combined review files into a single CSV file"""
    
    aggregate_dir = project_root / 'aggregate'
    aggregate_dir.mkdir(parents=True, exist_ok=True)  # Create aggregate directory if it doesn't exist
    
    output_path = aggregate_dir / 'combined_review_data.csv'
    
    logger.info("Aggregating combined review data from all apps...")
    
    all_reviews_dataframes = []
    
    for app_config in app_configs:
        app_path = app_config['app_path']
        processed_dir = project_root / 'data' / app_path / 'processed_data'
        combined_reviews_path = processed_dir / 'combined_reviews.csv'
        
        if combined_reviews_path.exists():
            try:
                logger.info(f"Reading data from {combined_reviews_path}")
                df = pd.read_csv(combined_reviews_path)
                
                # Add app name as an additional column for identification
                df['app_name'] = app_config['app_name']
                
                all_reviews_dataframes.append(df)
                logger.info(f"Successfully loaded {len(df)} reviews for {app_config['app_name']}")
            except Exception as e:
                logger.error(f"Error reading combined reviews for {app_config['app_name']}: {e}")
        else:
            logger.warning(f"No combined reviews file found for {app_config['app_name']} at {combined_reviews_path}")
    
    if all_reviews_dataframes:
        try:
            # Concatenate all dataframes
            combined_df = pd.concat(all_reviews_dataframes, ignore_index=True)
            
            # Save to CSV
            combined_df.to_csv(output_path, index=False)
            logger.info(f"Successfully aggregated all review data to {output_path}")
            logger.info(f"Total number of reviews: {len(combined_df)}")
            
            # Optional: Print stats about how many reviews from each app
            app_counts = combined_df['app_name'].value_counts()
            for app_name, count in app_counts.items():
                logger.info(f"{app_name}: {count} reviews")
                
            return True
        except Exception as e:
            logger.error(f"Error aggregating review data: {e}")
            logger.exception("Detailed traceback:")
            return False
    else:
        logger.error("No review data found to aggregate")
        return False

async def main():
    """Main ETL controller function that processes all apps"""
    config = load_config()
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        return
    
    apps = config.get('apps', [])
    if not apps:
        logger.error("No apps found in configuration. Exiting.")
        return
    
    start_time = time.time()
    
    # Process each app in the configuration
    results = []
    for app_config in apps:
        result = await process_app(app_config)
        results.append((app_config['app_name'], result))
    
    # Log summary of results
    for app_name, success in results:
        status = "SUCCESS" if success else "FAILED"
        logger.info(f"{app_name}: {status}")
    
    # Run S3 Backup before aggregation
    logger.info("Starting S3 backup before aggregation...")
    backup_success = run_s3_backup()
    if not backup_success:
        logger.warning("S3 backup failed, but continuing with aggregation...")
    
    # Aggregate all review data after processing all apps
    logger.info("Starting aggregation of all review data...")
    aggregate_success = aggregate_review_data(apps)
    if aggregate_success:
        logger.info("Successfully aggregated all review data")
    else:
        logger.error("Failed to aggregate review data")
    
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Total execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")
    logger.info("ETL process complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        logger.exception("Detailed traceback:")