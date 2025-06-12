import boto3
import re
from collections import defaultdict
from datetime import datetime

bucket_name = "etl-riesgo-penalizaciones-data"

s3 = boto3.client("s3")
paginator = s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=bucket_name)

# Detecta archivos: data.json, data.parquet, data_part1.json, etc.
pattern = re.compile(r"([^/]+)/day=(\d{2}-\d{2}-\d{4})/data(?:_part\d+)?\.(json|parquet)$")

summary = defaultdict(lambda: defaultdict(lambda: {"json": 0, "parquet": 0}))

for page in pages:
    for obj in page.get("Contents", []):
        key = obj["Key"]
        match = pattern.match(key)
        if match:
            collection, date_str, file_type = match.groups()
            summary[collection][date_str][file_type] += 1

# Imprime ordenado por fecha correctamente
for collection in summary:
    print(f"\n📁 Collection: {collection}")
    for date_str in sorted(summary[collection], key=lambda d: datetime.strptime(d, "%d-%m-%Y")):
        counts = summary[collection][date_str]
        print(f"  📅 {date_str} → JSON: {counts['json']}, Parquet: {counts['parquet']}")