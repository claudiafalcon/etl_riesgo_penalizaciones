
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
import bson



class MongoETLExtractor:
    def __init__(self, mongo_uri, bucket_name, collection, date_str, output_format="parquet"):
        self.mongo_uri = mongo_uri
        self.bucket_name = bucket_name
        self.output_format = output_format.lower()
        self.date_str = date_str
        self.collection = collection
        self.config = self._load_config()

        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.client.server_info()
            print("✅ Connected to MongoDB.")
        except Exception as e:
             print("❌ MongoDB connection failed:", e)
             raise
             
        self.db = self.client["EtominTransactions"]
        self.s3 = boto3.client("s3")

    def _delete_collection_data(self):
        prefix = f"{self.collection}/"  # Borra todo lo que esté bajo este prefijo
        print(f"🧹 Borrando todos los archivos en s3://{self.bucket_name}/{prefix} ...")

        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

        to_delete = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    to_delete.append({"Key": obj["Key"]})

        if to_delete:
            # Borrado en bloques de hasta 1000 objetos (límite de AWS)
            for i in range(0, len(to_delete), 1000):
                chunk = to_delete[i:i+1000]
                self.s3.delete_objects(Bucket=self.bucket_name, Delete={"Objects": chunk})
            print(f"✅ {len(to_delete)} objetos eliminados.")
        else:
            print("ℹ️ No se encontraron archivos para borrar.")

    def _load_config(self):
        try:
            with open(f"config/{self.collection}_config.json") as f:
                return json.load(f)
        except FileNotFoundError:
            raise ValueError(f"❌ Config not found for collection: {self.collection}")

    def _replace_placeholders(self,obj, start_ms, end_ms):
        if isinstance(obj, dict):
            return {
                k: self._replace_placeholders(v, start_ms, end_ms)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [self._replace_placeholders(i, start_ms, end_ms) for i in obj]
        elif isinstance(obj, str):
            if obj == "__start__":
                return start_ms
            elif obj == "__end__":
                return end_ms
        return obj



    def _paginated_cursor(self, collection, target_field, reference_ids, chunk_size=10000):
        for i in range(0, len(reference_ids), chunk_size):
            chunk = reference_ids[i:i + chunk_size]
            #print(f"🔄 Procesando chunk {i // chunk_size + 1} con {len(chunk)} elementos")
            for doc in self.db[collection].find({target_field: {"$in": chunk}}):
                yield doc
            
    def _build_cursor_with_config(self, start_ms, end_ms):
        mode = self.config.get("mode","delta")
        if mode == "replace":
            print(f"🧹 Mode is 'replace' → extracting entire collection '{self.collection}' in chunks")
            return self._paginated_cursor(self.collection, "_id", self._get_all_ids())
        
    # Si es delta, procesamos los filtros como antes
        filter_config = self.conf

        if "filter" in filter_config:
            if any(k in filter_config for k in ["filter_from_reference", "reference_from", "reference_field"]):
                raise ValueError(f"❌ Invalid fileter config for '{self.collection}': cannot mix 'filter' with reference-based fields")
            query = self._replace_placeholders(filter_config["filter"], start_ms, end_ms)
            print(f"This is the query :: '{query}'")
            return self.db[self.collection].find(query)

        elif "filter_from_reference" in filter_config:
            required_keys = {"reference_from", "reference_field"}
            if not required_keys.issubset(filter_config):
                raise ValueError(f"❌ Invalid config for '{self.collection}': missing 'reference_from' or 'reference_field'")

            ref_query = self._replace_placeholders(filter_config["filter_from_reference"], start_ms, end_ms)
            print(filter_config["reference_field"], ref_query)

            # Usa aggregate en lugar de distinct para evitar errores de 16MB
            reference_from = filter_config["reference_from"]
            reference_field = f"${filter_config['reference_field']}"

            pipeline = [
                {"$match": ref_query},
                {"$group": {"_id": reference_field}}
            ]

            reference_ids = [doc["_id"] for doc in self.db[reference_from].aggregate(pipeline)]

            if not reference_ids:
                print(f"⚠️ No referenced IDs found for {self.collection}, skipping...")
                return self.db[self.collection].find({"_id": {"$exists": False, "$eq": None}})

            target_field = filter_config.get("reference_target", "_id")
            return self._paginated_cursor(self.collection, target_field, reference_ids)
        elif "filterByIds" in filter_config:
            field = filter_config["filterByIds"].get("field", "_id")
            raw_values = filter_config["filterByIds"].get("values", [])

            # Convierte a ObjectId solo si el campo es _id
            values = [ObjectId(v) if field == "_id" else v for v in raw_values]

            print(f"🔍 Filtering collection '{self.collection}' by {field} with {len(values)} values")
            return self.db[self.collection].find({field: {"$in": values}})

        else:
            print(f"⚠️ No valid filter configuration found for '{self.collection}', returning empty cursor.")
            return self.db[self.collection].find({"_id": {"$exists": False, "$eq": None}})

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

            type_config = self.config("types",{})
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
    
    def extract_and_upload(self, date_str=None):
        from time import time
        try:
            start = time()
            date_str = date_str or self.date_str
            target_date = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = target_date + timedelta(days=1)
            start_ms = int(target_date.replace(tzinfo=timezone.utc).timestamp() * 1000)
            end_ms = int(next_day.replace(tzinfo=timezone.utc).timestamp() * 1000)
            if self.config.get("mode", "delta") == "replace":
                self._delete_collection_data()
            print(f"📦 Processing collection: {self.collection} for {date_str}")
            print(f"⏱ Timestamp range: {start_ms} to {end_ms}")
            blacklist = self.config.get("blacklist", [])

            cursor = self._build_cursor_with_config(start_ms,end_ms)
            batch = []
            batch_size = 1000
            doc_count = 0


            batch_index = 0  # 🆕 contador para el nombre del archivo
            while True:
                try:
                    doc = next(cursor)
                except StopIteration:
                    break
                except Exception as e:
                    print("❌ Error al obtener documento del cursor:", e)
                    continue

                try:
                    size = len(bson.BSON.encode(doc))
                    if size > 16 * 1024 * 1024:
                        print(f"🚨 Documento muy grande en {self.collection} para {date_str}, saltando: {doc.get('_id')}")
                        continue
                except Exception as e:
                    print("❌ Error al revisar tamaño del documento:", e)
                    continue

                batch.append(doc)
                if len(batch) >= batch_size:
                    self._process_batch(batch, self.collection, target_date, blacklist, batch_index)
                    doc_count += len(batch)
                    batch.clear()
                    batch_index += 1
            
            if batch:
                self._process_batch(batch, self.collection, target_date, blacklist, batch_index)
                doc_count += len(batch)

            print(f"📄 Processed {doc_count} documents in '{self.collection}' for {date_str}")
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

            print(f"⏱️ Elapsed time: {round(time() - start, 2)} seconds for {self.collection}/{target_date.strftime('day=%d-%m-%Y')}")
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
        

