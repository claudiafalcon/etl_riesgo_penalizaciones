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

def log_to_cloudwatch(message):
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    timestamp = int(time.time() * 1000)
    logs_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=LOG_STREAM,
        logEvents=[{"timestamp": timestamp, "message": message}]
    )



def get_blacklist(collection):
    try:
        with open(f'config/{collection}_blacklist.txt') as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()

def sanitize_document(doc, blacklist):
    for field in blacklist:
        keys = field.split(".")
        d = doc
        for k in keys[:-1]:
            d = d.get(k, {})
            if not isinstance(d, dict):
                d = {}
                break  # termina la cadena si ya no es un dict
        if isinstance(d, dict):
            d.pop(keys[-1], None)
    return doc

def extract_and_upload(date_str, collection, mongo_uri, bucket_name):
    start_time = time.time()
    client = pymongo.MongoClient(mongo_uri)

    try:
        client = pymongo.MongoClient(mongo_uri)
        client.server_info()  # Will throw an exception if cannot connect
        print("✅ Connected to MongoDB.")
    except Exception as e:
        print("❌ MongoDB connection failed:", e)
        return
    db = client["EtominTransactions"]
    s3 = boto3.client("s3")

    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = target_date + timedelta(days=1)


    start_ms = int(target_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(next_day.replace(tzinfo=timezone.utc).timestamp() * 1000)



    print(f"📦 Processing collection: {collection} for {date_str}")
    print(f"⏱ Timestamp range: {start_ms} to {end_ms}")

    blacklist = get_blacklist(collection)

    cursor = db[collection].find({
        "createdAt": {
            "$gte": start_ms,
            "$lt": end_ms
        }
    })

    docs = list(cursor)
    print(f"📄 Found {len(docs)} documents in '{collection}'")

    docs = [sanitize_document(doc, blacklist) for doc in docs]
    content = "\n".join(json.dumps(doc, default=str) for doc in docs)

    prefix = target_date.strftime("day=%d-%m-%Y")
    key = f"{collection}/{prefix}/data.json"
    s3.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))

    print(f"✅ Uploaded {len(docs)} docs to {key}")
    end_time = time.time()
    elapsed_time = round(end_time - start_time,2)
    log_to_cloudwatch(f"✅ Uploaded {len(docs)} docs to {key} in {elapsed_time} seconds.")

def main():

    parser = argparse.ArgumentParser(description="Extract one collection from MongoDB and upload to S3.")
    parser.add_argument("--date", required=True, help="Extraction date in format YYYY-MM-DD")
    parser.add_argument("--collection", required=True, choices=["transactionresponse", "sale", "seller"], help="Collection to extract")
    args = parser.parse_args()

    mongo_uri = os.environ.get("MONGO_URI")
    bucket_name = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")

    if not mongo_uri:
        raise ValueError("⚠️ MONGO_URI environment variable not set")

    extract_and_upload(args.date, args.collection, mongo_uri, bucket_name)

if __name__ == "__main__":
    main()