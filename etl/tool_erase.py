import boto3
from datetime import datetime, timezone, timedelta

bucket_name = "etl-riesgo-penalizaciones-data"
prefix = "transactionresponse"  # Si quieres borrar todo, déjalo vacío; puedes poner "transaction/" por ejemplo

s3 = boto3.client("s3")

cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)

# Listar objetos .parquet
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if "Contents" in response:
    archivos_antiguos = [
        obj['Key'] for obj in response['Contents']
        if obj['LastModified'] < cutoff_date &  obj["Key"].endswith(".parquet")
    ]
    
    print("Archivos NO actualizados en los últimos 3 días:")
    for archivo in archivos_antiguos:
        print(archivo)

    if archivos_antiguos["Objects"]:
        print(f"🧹 Deleting {len(archivos_antiguos['Objects'])} parquet files...")
       # s3.delete_objects(Bucket=bucket_name, Delete=archivos_antiguos)
        print("✅ Done.")
    else:
        print("No .parquet files found.")
else:
    print("Bucket is empty or prefix does not exist.")

