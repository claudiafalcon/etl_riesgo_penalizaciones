import subprocess
import time
from datetime import datetime, timedelta
import psutil

collections = ["transactionresponse", "sale", "seller"]
start_date = datetime.strptime("2025-05-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-05-03", "%Y-%m-%d")  # Cambia rango para pruebas

def print_mem_usage(label):
    process = psutil.Process()
    mem = process.memory_info()
    print(f"🧠 {label} — RSS: {mem.rss / (1024**2):.2f} MB, VMS: {mem.vms / (1024**2):.2f} MB")

current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    for collection in collections:
        print(f"\n🚀 Running ETL for {collection} on {date_str}")
        print_mem_usage("Before subprocess")

        try:
            subprocess.run(["python3", "./etl/bulk_one_day.py", date_str, collection], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Subprocess failed: {e}")
        
        time.sleep(1)  # pequeña pausa
        print_mem_usage("After subprocess")
    current += timedelta(days=1)

print("\n✅ Test completo.")