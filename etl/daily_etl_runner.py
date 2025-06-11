
import sys
from mongo_etl import MongoETLExtractor
import argparse
import os
import json
import boto3
import pymongo
import time
import logging
from datetime import datetime, timedelta, timezone

LOG_GROUP = "/etl/riesgo-penalizaciones"
LOG_STREAM = f"run-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M%S')}"
logs_client = boto3.client("logs", region_name = "us-east-1")

def log_duration(start, end, collection, date):
    duration = end - start
    print(f"🕒 Daily ETL  {collection} for {date} finished in {duration:.2f} seconds")

def main():

    parser = argparse.ArgumentParser(description="Extract one collection from MongoDB and upload to S3.")
    parser.add_argument("--date", required=True, help="Extraction date in format YYYY-MM-DD")
    parser.add_argument("--collection", required=True, choices=["transactionresponse", "sale", "seller"], help="Collection to extract")
    args = parser.parse_args()

    mongo_uri = os.environ.get("MONGO_URI")
    bucket_name = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")

    if not mongo_uri:
        raise ValueError("⚠️ MONGO_URI environment variable not set")
 
 
    extractor = MongoETLExtractor(mongo_uri, bucket_name)
    start_time = time.time()
    extractor.extract_and_upload(args.collection, args.date)
    end_time = time.time()
    log_duration(start_time, end_time,args.collection, args.date)

if __name__ == "__main__":
    main()
