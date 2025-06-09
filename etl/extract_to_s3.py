import argparse
import os
import json
import boto3
import pymongo
from datetime import datetime, timedelta

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
        d.pop(keys[-1], None)
    return doc

def extract_and_upload(date_str, mongo_uri, bucket_name):
    client = pymongo.MongoClient(mongo_uri)
    db = client["EtominTransactions"]
    s3 = boto3.client("s3")

    collections = ["transactionresponse", "sale", "seller"]
    target_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = target_date + timedelta(days=1)

    for coll in collections:
        print(f"📦 Processing collection: {coll}")
        blacklist = get_blacklist(coll)
        cursor = db[coll].find({
            "createdAt": {
                "$gte": target_date,
                "$lt": next_day
            }
        })
        docs = [sanitize_document(doc, blacklist) for doc in cursor]
        content = "\n".join(json.dumps(doc, default=str) for doc in docs)
        prefix = target_date.strftime("day=%d-%m-%Y")
        key = f"{coll}/{prefix}/data.json"
        s3.put_object(Bucket=bucket_name, Key=key, Body=content.encode("utf-8"))
        print(f"✅ Uploaded {len(docs)} docs to {key}")

def main():
    parser = argparse.ArgumentParser(description="Extract MongoDB data and upload to S3.")
    parser.add_argument("--date", required=True, help="Extraction date in format YYYY-MM-DD")
    args = parser.parse_args()

    mongo_uri = os.environ.get("MONGO_URI")
    bucket_name = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")

    if not mongo_uri:
        raise ValueError("⚠️ MONGO_URI environment variable not set")

    extract_and_upload(args.date, mongo_uri, bucket_name)

if __name__ == "__main__":
    main()