
# 📦 ETL Pipeline: Google Cloud Storage → BigQuery using Cloud Run (Eventarc)

## 📌 Overview

This lab implements an automated ETL pipeline that loads newline-delimited JSON (JSONL) files from **Google Cloud Storage (GCS)** into **BigQuery** using a serverless **Cloud Run function (2nd gen)** triggered by Eventarc.

When a file is uploaded to a storage bucket:

1. The trigger detects the object creation event
2. The Cloud Run function executes
3. The file is validated against predefined schemas
4. A BigQuery table is created (if it does not exist)
5. Data is loaded into BigQuery

---

## 🏗️ Architecture

```
User Upload → GCS Bucket
              ↓
     Eventarc Trigger (storage.objects.create)
              ↓
     Cloud Run Function (Python)
              ↓
     BigQuery Dataset (staging)
```

---

## 🛠️ Technologies Used

* Google Cloud Storage
* Cloud Run (2nd gen)
* Eventarc
* BigQuery
* Python 3.11
* Google Cloud SDK
* YAML (schema configuration)

---

## 📂 Project Structure

```
.
├── main.py
├── schemas.yaml
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Enable Required APIs

Enable the following APIs in Google Cloud Console:

* Cloud Run
* Cloud Functions
* Eventarc
* BigQuery
* Cloud Storage
* Pub/Sub

---

### 2️⃣ Create GCS Bucket

* Name: `my-etl-bucket-mlops`
* Region: `us-central1`
* Storage Class: Standard

---

### 3️⃣ Create BigQuery Dataset

* Dataset Name: `staging`
* Location: `us-central1` (must match bucket region)

---

### 4️⃣ Configure IAM Roles

Ensure the runtime service account has:

* `BigQuery Data Editor`
* `BigQuery Job User`
* `Eventarc Event Receiver`
* `Storage Object Viewer`

---

### 5️⃣ Deploy Cloud Run Function

Trigger Configuration:

* Event Provider: Cloud Storage
* Event Type: `storage.objects.create`
* Bucket: `my-etl-bucket-mlops`
* Region: `us-central1`
* Entry Point: `hello_auditlog`

Environment Variables:

```
BQ_DATASET=staging
BQ_LOCATION=us-central1
```

---

## 📄 schemas.yaml Format

```yaml
tables:
  - name: users
    format: NEWLINE_DELIMITED_JSON
    schema:
      - name: user_id
        type: STRING
        mode: REQUIRED
      - name: email
        type: STRING
        mode: NULLABLE
```

File naming convention:

```
users_1.jsonl
events_2026.jsonl
```

---

## 🧠 How It Works

1. Eventarc sends event when file is uploaded.
2. Function extracts:

   * Bucket name
   * Object name
3. Filename prefix determines target table.
4. Table is created if not existing.
5. Data is loaded using:

```python
bigquery.LoadJobConfig(
    source_format=NEWLINE_DELIMITED_JSON,
    write_disposition=WRITE_APPEND
)
```

---

## 🧪 Testing

Create a test file:

```
users_1.jsonl
```

Example content:

```json
{"user_id":"u1","email":"a@example.com"}
{"user_id":"u2","email":"b@example.com"}
```

Upload to bucket.

Verify in BigQuery:

```sql
SELECT * FROM `PROJECT_ID.staging.users`
```

---

## 🚀 Key Learnings

* Event-driven architecture with GCP
* Serverless ETL design
* BigQuery schema management
* Cloud Run + Eventarc integration
* JSONL ingestion best practices
* IAM role configuration troubleshooting

---

## ⚠️ Common Issues

| Issue                    | Cause                      | Fix                           |
| ------------------------ | -------------------------- | ----------------------------- |
| Trigger permission error | Missing Eventarc role      | Add `Eventarc Event Receiver` |
| BigQuery load error      | Region mismatch            | Match bucket + dataset region |
| Schema error             | Incorrect YAML             | Validate fields + modes       |
| JSON load failure        | Not newline-delimited JSON | Use JSONL format              |

---

## 🔄 Future Improvements

* Partitioned BigQuery tables
* Schema auto-detection option
* Dead-letter bucket for failed loads
* Data validation before ingestion
* Monitoring with Cloud Logging alerts

---

## 👩‍💻 Author

Suchitra Hole
MS Data Science – Northeastern University

