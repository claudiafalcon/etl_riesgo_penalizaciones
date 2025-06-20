import boto3
from datetime import datetime, timezone, timedelta

bucket_name = "etl-riesgo-penalizaciones-data"
prefix = "transactionresponse"  # Si quieres borrar todo, déjalo vacío; puedes poner "transaction/" por ejemplo

s3 = boto3.client("s3")

cutoff_date = datetime.now(timezone.utc) - timedelta(days=3)

# Listar objetos .parquet
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if 'Contents' in response:
    for obj in response['Contents']:
        if obj['LastModified'] < cutoff_date and obj['Key'].endswith('.parquet'):
            key = obj['Key']
            print(f"Borrando: {key}")
          #  s3.delete_object(Bucket=bucket_name, Key=key)
else:
    print("No se encontraron archivos con ese prefijo.")

