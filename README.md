# ETL Risk Penalties

This project extracts documents from a MongoDB database and uploads them to an S3 bucket per collection and per day, while maintaining NoSQL dynamism. It is optimized for execution from an EC2 instance with restricted access to MongoDB.

## 🗂 Project Structure

```
etl-risk-penalties/
├── config/
│   ├── transactionresponse_blacklist.txt
│   ├── sale_blacklist.txt
│   └── seller_blacklist.txt
├── extract_to_s3.py
├── requirements.txt
├── template.yaml
└── README.md
```

## ⚙️ Prerequisites

- Python 3.8+
- MongoDB access (read permissions)
- AWS CLI configured with permissions for S3
- Buckets created via `template.yaml` (using `sam deploy`)
- Mongo URI configured as environment variable

## 🧪 Configuration

1. **Install dependencies:**

```bash
pip install -r requirements.txt
```

2. **Set environment variables:**

```bash
export MONGO_URI="mongodb+srv://..."
export S3_BUCKET="etl-risk-penalties-data"
```

3. **Blacklist files:**

Each file in the `config/` folder lists the fields to exclude per collection (one per line, nested fields using dot notation).

## 🚀 Execution

```bash
python extract_to_s3.py --collection <collection> --date YYYY-MM-DD
```

Example:

```bash
python extract_to_s3.py --collection transactionresponse --date 2024-06-05
```

### Run in parallel (optional):

```bash
for c in transactionresponse sale seller; do
  python3 extract_to_s3.py --collection $c --date 2024-06-05 &
done
wait
```

## 📦 S3 Output

Documents are stored using the following key format:

```
<collection>/day=DD-MM-YYYY/data.json
```

Example:

```
sale/day=05-06-2024/data.json
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

---

## 📌 Notes

- The `createdAt` field is expected as a timestamp in milliseconds and converted to UTC.
- Only documents with `mode: "LIVE"` are considered (e.g., in the `sale` collection).

---
