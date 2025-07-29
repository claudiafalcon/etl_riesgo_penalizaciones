
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

def load_collections(config_path="config/collections.json"):
    with open(config_path) as f:
        data = json.load(f)
        return data.get("collections", [])

def main():

    parser = argparse.ArgumentParser(description="Extract one collection from MongoDB and upload to S3.")
    colecciones_validas = load_collections()
    parser.add_argument("--date", required=True, help="Extraction date in format YYYY-MM-DD")
    parser.add_argument("--collection", required=True, choices=colecciones_validas, help="Collection to extract")
    args = parser.parse_args()

    mongo_uri = os.environ.get("MONGO_URI")
    bucket_name = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")



    # Definir los formatos válidos
    VALID_OUTPUT_FORMATS = {"json", "parquet", "both"}

    # Obtener la variable de entorno o usar "parquet" por defecto
    output_format = os.getenv("OUTPUT_FORMAT", "parquet").lower()

# Validar formato
    if output_format not in VALID_OUTPUT_FORMATS:
        print(f"❌ Invalid OUTPUT_FORMAT: '{output_format}'. Must be one of: {', '.join(VALID_OUTPUT_FORMATS)}")
        raise ValueError(f"❌ Invalid OUTPUT_FORMAT: '{output_format}'. Must be one of: {', '.join(VALID_OUTPUT_FORMATS)}")


    if not mongo_uri:
        raise ValueError("⚠️ MONGO_URI environment variable not set")
 
 
    extractor = MongoETLExtractor(mongo_uri, bucket_name,args.collection, args.date, output_format)
    start_time = time.time()
    extractor.extract_and_upload()
    end_time = time.time()
    log_duration(start_time, end_time,args.collection, args.date)

if __name__ == "__main__":
    main()
