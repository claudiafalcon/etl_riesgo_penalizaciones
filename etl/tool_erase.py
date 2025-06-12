import boto3

bucket_name = "etl-riesgo-penalizaciones-data"
prefix = ""  # Si quieres borrar todo, déjalo vacío; puedes poner "transaction/" por ejemplo

s3 = boto3.client("s3")

# Listar objetos .parquet
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if "Contents" in response:
    to_delete = {
        "Objects": [
            {"Key": obj["Key"]}
            for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]
    }

    if to_delete["Objects"]:
        print(f"🧹 Deleting {len(to_delete['Objects'])} parquet files...")
        s3.delete_objects(Bucket=bucket_name, Delete=to_delete)
        print("✅ Done.")
    else:
        print("No .parquet files found.")
else:
    print("Bucket is empty or prefix does not exist.")