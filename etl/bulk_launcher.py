import subprocess
from datetime import datetime, timedelta
import sys
import time
import argparse
from multiprocessing import Pool
import psutil

# Lista oficial de colecciones válidas
colecciones_validas = ["transactionresponse", "sale", "seller", "chargeback", "refund"]

# Argumentos posicionales y opcional
parser = argparse.ArgumentParser(description="ETL launcher por lotes")

# Posicionales
parser.add_argument("start_date", help="Fecha de inicio en formato YYYY-MM-DD")
parser.add_argument("end_date", help="Fecha de fin en formato YYYY-MM-DD")
parser.add_argument("max_parallel", type=int, help="Máximo número de procesos en paralelo")

# Opcional
parser.add_argument("--collections", nargs="+", help="Colecciones a procesar (si se omite, se procesan todas)")

args = parser.parse_args()

# Validación de colecciones si se especifican
if args.collections:
    invalid = [c for c in args.collections if c not in colecciones_validas]
    if invalid:
        print(f"❌ Colecciones no válidas: {invalid}")
        print(f"✅ Colecciones válidas: {colecciones_validas}")
        sys.exit(1)
    colecciones_a_procesar = args.collections
else:
    colecciones_a_procesar = colecciones_validas

start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
max_parallel = args.max_parall


# Crear lista de tareas (combinación de fechas y colecciones)
tasks = []
current = start_date
while current <= end_date:
    date_str = current.strftime("%Y-%m-%d")
    for collection in colecciones_a_procesar:
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