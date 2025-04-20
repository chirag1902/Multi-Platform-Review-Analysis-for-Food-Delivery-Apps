#!/usr/bin/env python3
"""
S3 Backup Script - Uploads data from platform folders to Amazon S3
"""

import os
import sys
import boto3
import yaml
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# ───────────────────────────────────────────────────────────── #
# LOGGING SETUP
# ───────────────────────────────────────────────────────────── #

script_dir = Path(os.path.abspath(__file__)).parent
project_root = script_dir.parent if script_dir.name == "etl_scripts" else script_dir

log_dir = project_root / "logs"
log_dir.mkdir(exist_ok=True)
log_filename = f"s3_backup.log"
log_path = log_dir / log_filename

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("s3_backup")

# ───────────────────────────────────────────────────────────── #
# LOAD CONFIG (.env and config.yaml)
# ───────────────────────────────────────────────────────────── #

def load_config():
    """Load AWS credentials from .env and values from config.yaml"""
    try:
        # Load environment variables
        env_path = project_root / ".env"
        if not env_path.exists():
            logger.error(f".env file not found at {env_path}")
            return None
        load_dotenv(dotenv_path=env_path)

        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not aws_access_key_id or not aws_secret_access_key:
            logger.error("AWS credentials not found in .env file")
            return None

        # Load config.yaml
        config_path = project_root / "config" / "config.yaml"
        if not config_path.exists():
            logger.error(f"Config file not found at {config_path}")
            return None

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        aws_config = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": os.getenv("AWS_REGION", config["aws"]["region_name"]),
            "bucket_name": os.getenv("AWS_BUCKET_NAME", config["aws"]["bucket_name"])
        }

        return aws_config

    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return None

# ───────────────────────────────────────────────────────────── #
# AWS CLIENT & UPLOAD
# ───────────────────────────────────────────────────────────── #

def create_s3_client(aws_config):
    try:
        return boto3.client(
            's3',
            aws_access_key_id=aws_config['aws_access_key_id'],
            aws_secret_access_key=aws_config['aws_secret_access_key'],
            region_name=aws_config['region_name']
        )
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        return None

def upload_file_to_s3(s3_client, file_path, bucket_name, s3_key):
    try:
        logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Successfully uploaded {file_path} to S3")
        return True
    except ClientError as e:
        logger.error(f"Error uploading {file_path} to S3: {str(e)}")
        return False

# ───────────────────────────────────────────────────────────── #
# BACKUP LOGIC
# ───────────────────────────────────────────────────────────── #

def backup_platform_data(s3_client, bucket_name, data_dir, platform_name):
    logger.info(f"Starting backup for platform: {platform_name}")
    platform_dir = data_dir / platform_name
    if not platform_dir.exists():
        logger.warning(f"Platform directory {platform_dir} does not exist")
        return 0

    files_processed = 0

    for data_type in ["raw_data", "processed_data"]:
        sub_dir = platform_dir / data_type
        if sub_dir.exists():
            files_processed += process_directory(s3_client, bucket_name, sub_dir, platform_name, data_type)
        else:
            logger.warning(f"{data_type} directory {sub_dir} does not exist")

    logger.info(f"Completed backup for platform {platform_name} - {files_processed} files uploaded")
    return files_processed

def process_directory(s3_client, bucket_name, directory, platform_name, data_type):
    files_processed = 0

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, directory)
            s3_key = f"{platform_name}/{data_type}/{relative_path}"

            if upload_file_to_s3(s3_client, file_path, bucket_name, s3_key):
                files_processed += 1

    return files_processed

# ───────────────────────────────────────────────────────────── #
# MAIN
# ───────────────────────────────────────────────────────────── #

def main():
    logger.info("Starting S3 backup process...")

    aws_config = load_config()
    if not aws_config:
        logger.error("Failed to load AWS configuration. Exiting.")
        return 1

    s3_client = create_s3_client(aws_config)
    if not s3_client:
        logger.error("Failed to create S3 client. Exiting.")
        return 1

    bucket_name = aws_config["bucket_name"]
    data_dir = project_root / "data"

    if not data_dir.exists():
        logger.error(f"Data directory {data_dir} does not exist")
        return 1

    platforms = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

    if not platforms:
        logger.warning("No platform directories found in data directory")
        return 0

    total_files = 0
    for platform in platforms:
        files_processed = backup_platform_data(s3_client, bucket_name, data_dir, platform)
        total_files += files_processed

    logger.info(f"S3 backup process completed. Total files uploaded: {total_files}")
    return 0

