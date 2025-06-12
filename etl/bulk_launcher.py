import subprocess
import time
import os
from datetime import datetime, timedelta
import threading

# Configuración
collections = ["transactionresponse", "sale", "seller"]
heavy_collections = {"transactionresponse", "transaction"}  # Si agregas más, ponlas aquí
MAX_PARALLEL = 3

start_date = datetime.strptime("2025-06-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-06-10", "%Y-%m-%d")

semaphore = threading.Semaphore(MAX_PARALLEL)
lock = threading.Lock()

def run_one_day(date_str, collection):
    with semaphore:
        cmd = ["python3", "./etl/bulk_one_day.py", date_str, collection]
        print(f"🚀 Launching: {cmd}")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error: {e}")
        finally:
            print(f"✅ Finished: {collection} - {date_str}")

threads = []

current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    for collection in collections:
        # Si es heavy, espera a que todos terminen antes de continuar
        if collection in heavy_collections:
            for t in threads:
                t.join()
            threads = []

        t = threading.Thread(target=run_one_day, args=(date_str, collection))
        t.start()
        threads.append(t)

    current += timedelta(days=1)

# Esperar a que terminen todos los hilos
for t in threads:
    t.join()

print("✅ All tasks completed.")