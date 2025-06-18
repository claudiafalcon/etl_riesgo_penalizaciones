
import json
import boto3
import pymongo
from datetime import datetime, timedelta,timezone
from configparser import ConfigParser
from io import BytesIO
import pandas as pd
from bson import ObjectId
import gc
import psutil



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
    
    def _get_filter(self, collection):
        try:
            with open(f'config/{collection}_filter.json') as f:
                return json.load(f)
        except FileNotFoundError:
            raise ValueError(f"❌ No filter config fi found for collection: {collection}")
        
    def _build_cursor_with_config(self, collection, start_ms, end_ms):
        filter_config = self._get_filter(collection)
        if "filter" in filter_config:
            if any(k in filter_config for k in ["filter_from_reference", "reference_from", "reference_field"]):
                raise ValueError(f"❌ Invalid fileter config for '{collection}': cannot mix 'filter' with reference-based fields")
            query = json.loads(json.dumps(filter_config["filter"]).replace("__start__", str(start_ms)).replace("__end__", str(end_ms)))
            return self.db[collection].find(query)

        elif "filter_from_reference" in filter_config:
            required_keys = {"reference_from", "reference_field"}
            if not required_keys.issubset(filter_config):
                raise ValueError(f"❌ Invalid config for '{collection}': missing 'reference_from' or 'reference_field'")

            ref_query = json.loads(json.dumps(filter_config["filter_from_reference"]).replace("__start__", str(start_ms)).replace("__end__", str(end_ms)))
            reference_ids = self.db[filter_config["reference_from"]].distinct(filter_config["reference_field"], ref_query)

            if not reference_ids:
                print(f"⚠️ No referenced IDs found for {collection}, skipping...")
                return iter([])

            return self.db[collection].find({"_id": {"$in": reference_ids}})

    def _process_batch(self, docs, collection, target_date, blacklist, batch_index):
        
        sanitized_docs = [self._sanitize_document(doc, blacklist) for doc in docs]
    

        prefix = target_date.strftime("day=%d-%m-%Y")
   

        if self.output_format in ("json", "both"):
            content = "\n".join(json.dumps(doc, default=str) for doc in converted_docs)
            json_key = f"{collection}/{prefix}/data_part{batch_index + 1}.json"
            self.s3.put_object(Bucket=self.bucket_name, Key=json_key, Body=content.encode("utf-8"))
    
            del content

        if self.output_format in ("parquet", "both"):
            converted_docs = [self._convert_types(doc) for doc in sanitized_docs]
            df = pd.json_normalize(converted_docs)

            type_config = self._get_type_overrides(collection)
            for col in type_config.get("force_string", []):
                if col in df.columns:
                    df[col] = df[col].astype(str)
            for col in type_config.get("force_number", []):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            parquet_key = f"{collection}/{prefix}/data_part{batch_index + 1}.parquet"
            self.s3.put_object(Bucket=self.bucket_name, Key=parquet_key, Body=buffer.getvalue())

            buffer.close()
            del buffer

            del df

        del sanitized_docs
        del converted_docs
        del docs
        gc.collect()


    def _get_type_overrides(self, collection):
        try:
            with open(f'config/{collection}_types.json') as f:
                return json.load(f)
        except FileNotFoundError:
            return {"force_string": [], "force_number": []}
        
    def _convert_types(self, doc):
        def convert_value(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(v) for v in value]
            else:
                return value  # dejar en su tipo original

        return {k: convert_value(v) for k, v in doc.items()}


        
    def _get_blacklist(self, collection):
        try:
            with open(f'config/{collection}_blacklist.txt') as f:
                return set(line.strip() for line in f if line.strip())
        except FileNotFoundError:
            return set()

    def _sanitize_document(self,doc, blacklist):
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
        try:
            start = time()

            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = target_date + timedelta(days=1)
            start_ms = int(target_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
            end_ms = int(next_day.replace(tzinfo=timezone.utc).timestamp() * 1000)


            print(f"📦 Processing collection: {collection} for {date_str}")
            print(f"⏱ Timestamp range: {start_ms} to {end_ms}")
            blacklist = self._get_blacklist(collection)

            cursor = self._build_cursor_with_config(collection,start_ms,end_ms)
            batch = []
            batch_size = 1000
            doc_count = 0


            batch_index = 0  # 🆕 contador para el nombre del archivo

            for doc in cursor:
                batch.append(doc)
                if len(batch) >= batch_size:
                    self._process_batch(batch, collection, target_date, blacklist, batch_index)
                    doc_count += len(batch)
                    batch.clear()
                    batch_index += 1  # 🆙 siguiente batch

            # Procesar los que queden
            if batch:
                self._process_batch(batch, collection, target_date, blacklist, batch_index)
                doc_count += len(batch)

            print(f"📄 Processed {doc_count} documents in '{collection}' for {date_str}")
            cursor.close()
            del cursor

            def log_large_objects(min_size_mb=0.1):
                print("🔍 Buscando objetos grandes en memoria...")
                count = 0
                for obj in gc.get_objects():
                    try:
                        size_mb = sys.getsizeof(obj) / (1024 ** 2)
                        if size_mb > min_size_mb:
                            print(f"🧱 Tipo: {type(obj)}, Tamaño: {size_mb:.2f} MB")
                            count += 1
                            if count >= 10:  # límite para evitar spam
                                break
                    except Exception:
                        pass
            import gc
            import sys

            def debug_large_objects(threshold_mb=5):
                print("🔍 Escaneando objetos grandes en memoria:")
                for obj in gc.get_objects():
                    try:
                        size = sys.getsizeof(obj)
                        if size > threshold_mb * 1024 * 1024:
                            print(f"🧱 Tipo: {type(obj)} — Tamaño: {size / (1024**2):.2f} MB")
                    except Exception:
                        pass

            print(f"⏱️ Elapsed time: {round(time() - start, 2)} seconds for {collection}/{target_date.strftime('day=%d-%m-%Y')}")
            log_large_objects(min_size_mb=1)
            mem = psutil.virtual_memory()
        
            print(f"⏱️ Mem usage before cleanup: {mem.percent}% ({mem.used / (1024**2):.2f} MB)")

            process = psutil.Process()
            mem_info = process.memory_info()
            print(f"🧠 RSS: {mem_info.rss / (1024 ** 2):.2f} MB, VMS: {mem_info.vms / (1024 ** 2):.2f} MB)")
            self.client.close()
            del self.s3
            gc.collect()
            debug_large_objects()
            mem = psutil.virtual_memory()
            print(f"🧠 Mem usage after cleanup: {mem.percent}% ({mem.used / (1024**2):.2f} MB)")
            log_large_objects(min_size_mb=0.1)
        finally: 
            if hasattr(self, "client"):
                self.client.close()
            if hasattr(self, "s3"):
                del self.s3
            gc.collect()
            debug_large_objects()
            mem = psutil.virtual_memory()
            print(f"🧠 Mem usage after cleanup: {mem.percent}% ({mem.used / (1024**2):.2f} MB)")
            log_large_objects(min_size_mb=0.1)
        

