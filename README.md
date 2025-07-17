# MongoDB to S3 ETL – Configurable, Secure, and Cloud-Ready  
**Study Case: ETL Risk Penalties**

This project implements a flexible and secure ETL pipeline that extracts documents from a MongoDB database, applies configurable filters and transformations, and uploads the results to an S3 bucket in either JSON or Parquet format. It is optimized for execution from an EC2 instance with restricted access to MongoDB (e.g., fixed IP firewall rules).

---

## 🗂 Project Structure
```
etl-risk-penalties/
├── config/
│   ├── <collection>_blacklist.txt   # Fields to exclude
│   ├── <collection>_filter.json     # Filter logic
│   ├── <collection>_types.json      # Type enforcement
│   ├── collections.json             # List of collections for bulk mode
├── etl/
│   ├──daily_etl_runner.py # Scripts to load daily bases a specific collection
│   ├──bulk_launcher.py # Runs in parallel for range of dates and for more than one collection, depends of instnace resources.
├── etl/requirements.txt
├── infra/template.yaml
└── README.md
```

---

## ⚙️ Requirements

- Python 3.8+
- MongoDB read access
- AWS S3 bucket (created via template or manually)
- EC2 instance with an IAM role that grants access to S3
- MongoDB URI as environment variable

---

## 🔐 Secure MongoDB Access

This ETL is designed to run from a fixed-IP EC2 instance when MongoDB access is firewall-restricted.  
Authentication with AWS is handled through the EC2 IAM Role (no need to store AWS credentials).

---

## 🧪 Setup & Configuration

1. **Install dependencies**

```bash
pip install -r etl/requirements.txt
```

2. Set 🌍 Environment Variables


The ETL requires the following environment variables:

| Variable        | Description                                             | Example                      |
|----------------|---------------------------------------------------------|------------------------------|
| `MONGO_URI`     | MongoDB connection string                              | `mongodb+srv://user@...`     |
| `S3_BUCKET`     | Target S3 bucket name                                  | `etl-risk-data`              |
| `OUTPUT_FORMAT` | Output format: `parquet`, `json`, or `both`            | `parquet`                    |

If `OUTPUT_FORMAT` is set to `both`, the script will upload both JSON and Parquet versions.

3. **Config files:**

Each file  `config/<collection>_blacklist` lists the fields to exclude per collection (one per line, nested fields using dot notation).
Each file  `config/<collection>_filter` specify simple filter rules to extract data from each collection.
Each file  `config/<collection>_types` specify fields should force to be treated as string or number
File `config/collection.json` is the one used for bulk version to get the collection to be extracted.

4. ## 🚀 Execution

```bash
python3 etl/daily_etl_runner.py  --collection <collection> --date YYYY-MM-DD
```

Example:

```bash
python3 etl/daily_etl_runner.py --date 2025-06-06 --collection refund
```

## 🧠 Batch Mode (Parallel Collection Processing)

For large-scale execution, the ETL includes a bulk mode to extract multiple collections in parallel.

Use the included script (e.g., bulk_extract.py) to process multiple collections with optimized concurrency:

```bash
python3 etl/bulk_launcher.py <start_date> <end_date> <max_parallel> --collections <col1> <col2> ...
```

Mandatory arguments start_date(first date to load) and end_date (last date to load). 

This script:
	•	Loads a list of collections from --collections argument or load for a config file stored in 'config/collections.json'
	•	Executes each extraction in a separate  process
	•	Can significantly reduce total runtime for full-day extractions

Example:

```bash
python3 ./etl/bulk_launcher.py 2025-06-21 2025-07-17 2 --collections sale refund chargeback
```

## 📦 S3 Output

Documents are stored using the following key format :

```
<collection>/day=DD-MM-YYYY/data_<part>.parquet
```

Example:

```
sale/day=05-06-2024/data_1.parquet
```

## 🛠 Infrastructure (SAM)

The `template.yaml` file deploys:

- S3 buckets per collection
- IAM roles for read/write access to S3
- CloudWatch log group for monitoring

Deploy with:

```bash
sam deploy --guided
```

## 📊 Monitoring

Each run logs:

- The collection name
- The processed date
- Number of documents uploaded
- S3 key of the uploaded file

Logs are automatically sent to CloudWatch if running from EC2 with the correct IAM role.

## 📌 Notes

- Filtering by date assumes a `createdAt` field in milliseconds (converted to UTC).
- Cross-collection filtering supported via reference IDs.
- Transformation logic is fully configurable via JSON — no code change needed.

⸻

##✨ Ideal For
	•	Analysts and data engineers working with MongoDB in cloud environments
	•	Secure ETL workflows where fixed IP access to MongoDB is required
	•	Teams preparing data for machine learning, analytics, or compliance audits

⸻

##🤝 Need help?
Contact the author for:
- Customization
- Deployment automation (e.g., cron, user_data)
- Integration into APIs or dashboards