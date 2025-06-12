import boto3
from collections import defaultdict
import re

s3 = boto3.client("s3")
bucket_name = "etl-riesgo-penalizaciones-data"

pattern = re.compile(r"([^/]+)/day=(\d{2}-\d{2}-\d{4})/data\.(json|parquet)$")
summary = defaultdict(lambda: defaultdict(lambda: {"json": 0, "parquet": 0}))

continuation_token = None
while True:
    kwargs = {"Bucket": bucket_name}
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    response = s3.list_objects_v2(**kwargs)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        match = pattern.match(key)
        if match:
            collection, date_str, ext = match.groups()
            summary[collection][date_str][ext] += 1

    if response.get("IsTruncated"):
        continuation_token = response["NextContinuationToken"]
    else:
        break

# Mostrar resumen ordenado
for collection in sorted(summary):
    print(f"\n📁 Collection: {collection}")
    for date_str in sorted(summary[collection]):
        counts = summary[collection][date_str]
        print(f"  📅 {date_str}: JSON={counts['json']} | Parquet={counts['parquet']}")