# bulk_one_day.py
import os
import sys
from datetime import datetime
from mongo_etl import MongoETLExtractor  # Tu clase actual, sin cambios

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bulk_one_day.py <YYYY-MM-DD> <collection>")
        sys.exit(1)

    date_str = sys.argv[1]
    collection = sys.argv[2]

    extractor = MongoETLExtractor(
        mongo_uri=os.environ["MONGO_URI"],
        bucket_name=os.environ.get("S3_BUCKET", "etl-riesgo-penalizaciones-data"),
        output_format=os.environ.get("OUTPUT_FORMAT", "parquet")
    )

    extractor.extract_and_upload(collection, date_str)