
import json
import boto3
import pymongo
from datetime import datetime, timedelta,timezone
from configparser import ConfigParser
from io import BytesIO
import pandas as pd
from bson import ObjectId



class MongoETLExtractor:
    def __init__(self, mongo_uri, bucket_name, output_format="parquet"):
        self.mongo_uri = mongo_uri
        self.bucket_name = bucket_name
        self.output_format = output_format.lower()

        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.client.server_info()
            print("✅ Connected to MongoDB.")
        except Exception as e:
             print("❌ MongoDB connection failed:", e)
             raise
             
        self.db = self.client["EtominTransactions"]
        self.s3 = boto3.client("s3")
    


    def convert_types(self, doc):
    

        def convert_value(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(v) for v in value]
            elif isinstance(value, (int, float, bool)):
                return value
            else:
                return str(value)

        return {k: convert_value(v) for k, v in doc.items()}
    
 

        
    def get_blacklist(self, collection):
        try:
            with open(f'config/{collection}_blacklist.txt') as f:
                return set(line.strip() for line in f if line.strip())
        except FileNotFoundError:
            return set()

    def sanitize_document(self,doc, blacklist):
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
    
    def extract_and_upload(self, collection, date_str):
        from time import time
        start = time()

        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        next_day = target_date + timedelta(days=1)
        start_ms = int(target_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms = int(next_day.replace(tzinfo=timezone.utc).timestamp() * 1000)


        print(f"📦 Processing collection: {collection} for {date_str}")
        print(f"⏱ Timestamp range: {start_ms} to {end_ms}")
        blacklist = self.get_blacklist(collection)

        cursor = self.db[collection].find({
            "$or": [
                {
                    "updatedAt": {
                        "$gte": start_ms,
                        "$lt": end_ms
                    }
                },
                {
                    "updatedAt": { "$exists": False },
                    "createdAt": {
                        "$gte": start_ms,
                        "$lt": end_ms
                    }
                }
            ]
        })
        docs = list(cursor)
        print(f"📄 Found {len(docs)} documents in '{collection}'")

        sanitized_docs = [self.sanitize_document(doc, blacklist) for doc in docs]
        prefix = target_date.strftime("day=%d-%m-%Y")
        
        print(f"✅ Bucket {self.bucket_name} Prefix {prefix}")
        if self.output_format in ("json","both"):
            content = "\n".join(json.dumps(doc, default=str) for doc in sanitized_docs)
            json_key = f"{collection}/{prefix}/data.json"
            print(f"✅ Bucket {self.bucket_name} Prefix {prefix} docs to {json_key}")
            self.s3.put_object(Bucket=self.bucket_name, Key=json_key, Body=content.encode("utf-8"))
            print(f"✅ Uploaded {len(sanitized_docs)} JSON docs to {json_key}")



        if self.output_format in ("parquet", "both"):
            converted_docs = [self.convert_types(doc) for doc in sanitized_docs]
            df = pd.json_normalize(converted_docs)
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_key = f"{collection}/{prefix}/data.parquet"
            self.s3.put_object(Bucket=self.bucket_name, Key=parquet_key, Body=buffer.getvalue())
            print(f"✅ Uploaded {len(sanitized_docs)} Parquet docs to {parquet_key}")
        print(f"⏱️ Elapsed time: {round(time() - start, 2)} seconds")
