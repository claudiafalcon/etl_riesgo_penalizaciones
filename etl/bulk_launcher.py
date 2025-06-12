import subprocess
from datetime import datetime, timedelta
import sys
import time
from multiprocessing import Pool
import psutil

# 👉 Argumentos: start_date, end_date, max_processes
if len(sys.argv) < 4:
    print("❗ Uso: python3 bulk_launcher.py <start_date> <end_date> <max_parallel>")
    print("📅 Ejemplo: python3 bulk_launcher.py 2025-06-01 2025-06-10 2")
    sys.exit(1)

start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d")
max_parallel = int(sys.argv[3])

collections = ["transactionresponse", "sale", "seller"]

# Crear lista de tareas (combinación de fechas y colecciones)
tasks = []
current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    for collection in collections:
        tasks.append((date_str, collection))
    current += timedelta(days=1)

# Función que corre el ETL como subprocess


def run_etl(task):
    date_str, collection = task
    cmd = ["python3", "./etl/bulk_one_day.py", date_str, collection]
    print(f"\n🚀 Running ETL for {collection} on {date_str}")

    # Memoria antes del subprocess
    process = psutil.Process()
    mem_info_before = process.memory_info()
    print(f"🧠 Before subprocess — RSS: {mem_info_before.rss / (1024 ** 2):.2f} MB, VMS: {mem_info_before.vms / (1024 ** 2):.2f} MB")

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")

    # Memoria después del subprocess
    mem_info_after = process.memory_info()
    print(f"🧠 After subprocess — RSS: {mem_info_after.rss / (1024 ** 2):.2f} MB, VMS: {mem_info_after.vms / (1024 ** 2):.2f} MB")
    print(f"✅ Finished ETL for {collection} on {date_str}")

# Ejecutar en paralelo
if __name__ == "__main__":
    print(f"🔧 Starting ETL with max {max_parallel} parallel processes...")
    start = time.time()
    with Pool(processes=max_parallel) as pool:
        pool.map(run_etl, tasks)
    print(f"🏁 All ETL tasks completed in {round(time.time() - start, 2)} seconds.")