import os
import time
import boto3
import psutil
import logging
from datetime import datetime
from mongo_etl import MongoETLExtractor
from concurrent.futures import ThreadPoolExecutor

# Read environment variables
MONGO_URI = os.environ.get("MONGO_URI")
BUCKET_NAME = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")
LOG_GROUP = os.environ.get("LOG_GROUP_NAME", "/etl/riesgo-penalizaciones")
LOG_STREAM = f"bulk_loader_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
MAX_THREADS = 3
MEMORY_THRESHOLD = 70  # %
# Prepare CloudWatch Logs
logs_client = boto3.client("logs", region_name=os.environ.get("AWS_REGION", "us-east-1"))

def log_memory_usage():
    memory = psutil.virtual_memory()
    logging.info(f"📊 Memory Used: {memory.used / (1024 ** 2):.2f} MB / {memory.total / (1024 ** 2):.2f} MB")

def is_memory_safe(threshold=70):
    return psutil.virtual_memory().percent < threshold

def init_log_stream():
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

def put_log(message, timestamp=None):
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    global sequence_token
    kwargs = {
        "logGroupName": LOG_GROUP,
        "logStreamName": LOG_STREAM,
        "logEvents": [{"timestamp": timestamp, "message": message}]
    }
    if sequence_token:
        kwargs["sequenceToken"] = sequence_token
    response = logs_client.put_log_events(**kwargs)
    sequence_token = response.get("nextSequenceToken")

def run_etl_for_day_and_collection(date_str, collection):
    etl = MongoETLExtractor(MONGO_URI, BUCKET_NAME)
    etl.extract_and_upload(collection, date_str)

if __name__ == "__main__":
    from datetime import timedelta

    import sys
   
    collections = ["transactionresponse", "sale", "seller"]



    if len(sys.argv) < 3:
        print("Usage: python bulk_runner.py <start_date:YYYY-MM-DD> <end_date:YYYY-MM-DD> [max_threads]")
        sys.exit(1)

    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
    threads = int(sys.argv[3]) if len(sys.argv) > 3 else 3



    init_log_stream()
    sequence_token = None

    start_ts = time.time()
    put_log(f"🚀 Starting bulk extraction from {start_date} to {end_date} using {threads} threads")

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []
        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            for collection in collections:
                while not  is_memory_safe(MEMORY_THRESHOLD):
                    print("⚠️ High memory usage — waiting...")
                    time.sleep(5)
                print(f"🚀 Launching thread for {collection} - {date_str}")
                futures.append(executor.submit(run_etl_for_day_and_collection, date_str, collection))
                log_memory_usage()
            current += timedelta(days=1)

        for future in futures:
            future.result()

    elapsed = round(time.time() - start_ts, 2)
    put_log(f"✅ Bulk ETL completed in {elapsed} seconds")
