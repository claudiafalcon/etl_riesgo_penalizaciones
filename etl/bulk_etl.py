import os
import time
import boto3
import psutil
import logging
import threading
from datetime import datetime, timedelta, timezone
from mongo_etl import MongoETLExtractor

# Configuraciones
MONGO_URI = os.environ.get("MONGO_URI")
BUCKET_NAME = os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data")
LOG_GROUP = os.environ.get("LOG_GROUP_NAME", "/etl/riesgo-penalizaciones")
OUTPUT_FORMAT = os.environ.get("OUTPUT_FORMAT", "parquet")
LOG_STREAM = f"bulk_loader_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
MAX_THREADS = int(os.environ.get("MAX_THREADS", 3))
MEMORY_THRESHOLD = int(os.environ.get("MEMORY_THRESHOLD", 70))

collections = ["transactionresponse", "sale", "seller"]
heavy_collections = {"transactionresponse", "transaction"}  # Puedes agregar más

logs_client = boto3.client("logs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
sequence_token = None


def init_log_stream():
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass


def put_log(message, timestamp=None):
    global sequence_token
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    kwargs = {
        "logGroupName": LOG_GROUP,
        "logStreamName": LOG_STREAM,
        "logEvents": [{"timestamp": timestamp, "message": message}]
    }
    if sequence_token:
        kwargs["sequenceToken"] = sequence_token
    response = logs_client.put_log_events(**kwargs)
    sequence_token = response.get("nextSequenceToken")


def log_memory_usage():
    memory = psutil.virtual_memory()
    put_log(f"\uD83D\uDCCA Memory Used: {memory.used / (1024 ** 2):.2f} MB / {memory.total / (1024 ** 2):.2f} MB")


def is_memory_safe(threshold):
    log_memory_usage()
    return psutil.virtual_memory().percent < threshold


active_threads = []
thread_lock = threading.Lock()



heavy_running_count = 0  # reemplaza heavy_running

def run_etl_thread(date_str, collection):
    global heavy_running_count
    is_heavy = collection in heavy_collections
    with thread_lock:
        if is_heavy:
            heavy_running_count += 1
    try:
        etl = MongoETLExtractor(MONGO_URI, BUCKET_NAME, OUTPUT_FORMAT)
        etl.extract_and_upload(collection, date_str)
    except Exception as e:
        put_log(f"❌ Error in {collection} for {date_str}: {e}")
    finally:
        with thread_lock:
            if is_heavy:
                heavy_running_count -= 1

def clean_finished_threads():
    global active_threads
    with thread_lock:
        before = len(active_threads)
        active_threads = [t for t in active_threads if t.is_alive()]
        after = len(active_threads)
        if before != after:
            print(f"🧹 Cleaned up {before - after} finished threads.")


def wait_for_resources(collection, date_str, max_wait_seconds=300):
    waited = 0
    interval = 5
    while waited < max_wait_seconds:
        clean_finished_threads()
        with thread_lock:
            safe_memory = is_memory_safe(MEMORY_THRESHOLD)
            too_many_threads = len(active_threads) >= MAX_THREADS
            heavy_conflict = heavy_running_count > 0 and collection in heavy_collections

            if safe_memory and not too_many_threads and not heavy_conflict:
                return

        print(f"⚠️ Waiting for resources... ({collection} on {date_str}) [{waited}s elapsed]")
        print(f"🧮 Active threads: {len(active_threads)}, Heavy running: {heavy_running_count}")
        time.sleep(interval)
        waited += interval

        if waited % 30 == 0:
            print("🧵 Estado de los hilos activos:")
            with thread_lock:
                for t in active_threads:
                    print(f"   - Thread {t.name} alive: {t.is_alive()}")

    msg = f"⏱️ Timeout waiting for resources — skipping {collection} on {date_str}"
    print(msg)
    put_log(msg)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python bulk_runner.py <start_date:YYYY-MM-DD> <end_date:YYYY-MM-DD>")
        sys.exit(1)

    start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")

    init_log_stream()
    put_log(f"\uD83D\uDE80 Starting bulk extraction from {start_date} to {end_date} using max {MAX_THREADS} threads")

    start_ts = time.time()
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        for collection in collections:
            wait_for_resources(collection, date_str)
            t = threading.Thread(target=run_etl_thread, args=(date_str, collection))
            t.start()
            active_threads.append(t)
            put_log(f"\uD83D\uDE80 Launching thread for {collection} - {date_str}")
        current += timedelta(days=1)

    for t in active_threads:
        t.join()

    elapsed = round(time.time() - start_ts, 2)
    put_log(f"✅ Bulk ETL completed in {elapsed} seconds")