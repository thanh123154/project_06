# 🚀 HƯỚNG DẪN HOÀN CHỈNH: Deploy & Chạy BigQuery Auto Loader

## 📋 OVERVIEW - Tổng quan project

Project này có **3 thành phần chính**:

```
┌─────────────────────────────────────────────────────────────┐
│  1. export_to_gcs.py                                        │
│     ↓ Exports từ MongoDB → GCS                              │
│                                                              │
│  2. bq/cloud_functions/bq_auto_loader/                      │
│     ↓ Cloud Function (Gen1) tự động load GCS → BigQuery    │
│                                                              │
│  3. bq/scripts/load_jsonl_from_gcs.py                       │
│     ↓ Script manual load (backup)                          │
└─────────────────────────────────────────────────────────────┘
```

### Workflow hoàn chỉnh:

```
MongoDB → export_to_gcs.py → GCS → bq_auto_loader → BigQuery
```

---

## 🔍 PHẦN 1: CẤU TRÚC PROJECT

### Files và thư mục:

```
project_06/
├── export_to_gcs.py                    # Script export MongoDB → GCS
├── exporter/                           # Module export
│   ├── mongo_exporter.py              # MongoDB connection & export
│   ├── gcs.py                         # GCS upload
│   ├── writers.py                     # File writers (JSONL, CSV, etc.)
│   └── utils.py                       # Utilities
│
├── schema/
│   └── glamira_schema_raw.json        # BigQuery schema
│
├── bq/
│   ├── cloud_functions/
│   │   ├── bq_auto_loader/           # ✅ Cloud Function chính
│   │   │   ├── main.py               # Function code
│   │   │   ├── deploy.sh             # Deploy script
│   │   │   ├── requirements.txt       # Dependencies
│   │   │   └── schema.json           # Schema (copy từ root)
│   │   └── bq_loader/                # Alternative (không dùng)
│   └── scripts/
│       └── load_jsonl_from_gcs.py    # Manual load script
│
└── requirements.txt                    # Main dependencies
```

---

## 🛠️ PHẦN 2: SETUP BAN ĐẦU

### Bước 1: Install dependencies

```bash
# Kích hoạt venv (nếu có)
.\venv\Scripts\activate

# Install packages cho export script
pip install -r requirements.txt

# Install packages cho Cloud Function
pip install -r bq/cloud_functions/bq_auto_loader/requirements.txt
```

### Bước 2: Setup Google Cloud

```bash
# Login
gcloud auth login

# Set project
gcloud config set project consummate-rig-466909-i6

# Bật các APIs cần thiết
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### Bước 3: Setup Service Account

```bash
# Set credentials cho export script
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service-account-key.json"
```

**Hoặc thêm vào .env:**

```env
GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account-key.json
```

---

## 🚀 PHẦN 3: DEPLOY CLOUD FUNCTION

### ⚙️ Thông tin quan trọng:

| Item              | Value                                     |
| ----------------- | ----------------------------------------- |
| **Function Name** | `bq-auto-loader`                          |
| **Runtime**       | Python 3.11                               |
| **Trigger**       | GCS Object Finalize                       |
| **Bucket**        | `first-bucket-practice-for-data-engineer` |
| **Path Pattern**  | `exports/daily/*.jsonl`                   |
| **Region**        | `us-central1`                             |
| **Memory**        | 1GB                                       |
| **Timeout**       | 1800s (30 min)                            |
| **Project**       | `consummate-rig-466909-i6`                |
| **Dataset**       | `my_raw_dataset`                          |
| **Table**         | `events2`                                 |

---

### CÁCH 1: Deploy bằng script (Khuyến nghị)

#### Trên Linux/Mac:

```bash
cd bq/cloud_functions/bq_auto_loader

# Cấp quyền execute
chmod +x deploy.sh

# Chạy deploy
./deploy.sh
```

#### Script sẽ làm gì:

```bash
# 1. Copy schema từ root
cp ../../../schema/glamira_schema_raw.json ./schema.json

# 2. Deploy Cloud Function
gcloud functions deploy bq-auto-loader \
  --runtime python311 \
  --trigger-event-type google.storage.object.finalize \
  --trigger-resource first-bucket-practice-for-data-engineer \
  --trigger-event-filters path-pattern="exports/daily/*.jsonl" \
  --set-env-vars PROJECT_ID=consummate-rig-466909-i6 \
  --set-env-vars DATASET=my_raw_dataset \
  --set-env-vars TABLE=events2 \
  --set-env-vars WRITE_DISPOSITION=WRITE_APPEND \
  --set-env-vars MAX_BAD_RECORDS=1000 \
  --set-env-vars SCHEMA_PATH=/workspace/schema.json \
  --memory=1GB \
  --timeout=1800s \
  --max-instances=10 \
  --region=us-central1

# 3. Test với file mẫu
echo '{"test": "data"}' | gsutil cp - "gs://bucket/exports/daily/test.jsonl"
```

---

### CÁCH 2: Deploy thủ công trên Windows

#### Bước 1: Copy schema

```powershell
# Từ project root
Copy-Item schema\glamira_schema_raw.json bq\cloud_functions\bq_auto_loader\schema.json
```

#### Bước 2: Deploy function

```powershell
cd bq\cloud_functions\bq_auto_loader

gcloud functions deploy bq-auto-loader `
  --runtime python311 `
  --trigger-event-type google.storage.object.finalize `
  --trigger-resource first-bucket-practice-for-data-engineer `
  --trigger-event-filters path-pattern="exports/daily/*.jsonl" `
  --set-env-vars PROJECT_ID=consummate-rig-466909-i6 `
  --set-env-vars DATASET=my_raw_dataset `
  --set-env-vars TABLE=events2 `
  --set-env-vars WRITE_DISPOSITION=WRITE_APPEND `
  --set-env-vars MAX_BAD_RECORDS=1000 `
  --set-env-vars SCHEMA_PATH=/workspace/schema.json `
  --memory=1GB `
  --timeout=1800s `
  --max-instances=10 `
  --region=us-central1
```

---

## 🧪 PHẦN 4: TEST FUNCTION

### Test 1: Upload file để trigger

```powershell
# Upload file lên GCS
gsutil cp test_file.jsonl gs://first-bucket-practice-for-data-engineer/exports/daily/test.jsonl
```

### Test 2: Xem logs

```powershell
# Xem logs gần nhất
gcloud functions logs read bq-auto-loader --region=us-central1 --limit=20

# Xem logs real-time
gcloud functions logs read bq-auto-loader --region=us-central1 --follow

# Xem logs errors only
gcloud functions logs read bq-auto-loader --region=us-central1 --severity=ERROR --limit=50
```

### Test 3: Kiểm tra BigQuery

```powershell
# Xem table
bq show consummate-rig-466909-i6:my_raw_dataset.events2

# Query số lượng records
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`consummate-rig-466909-i6.my_raw_dataset.events2\`"

# Xem 10 rows mới nhất
bq query --use_legacy_sql=false "SELECT * FROM \`consummate-rig-466909-i6.my_raw_dataset.events2\` ORDER BY time_stamp DESC LIMIT 10"
```

---

## 🎯 PHẦN 5: WORKFLOW HOÀN CHỈNH

### End-to-end pipeline:

```bash
# BƯỚC 1: Export từ MongoDB → GCS
python export_to_gcs.py `
  --db your_database `
  --collection your_collection `
  --query-file sample_query.json `
  --format jsonl `
  --batch-size 5000 `
  --gcs-bucket first-bucket-practice-for-data-engineer `
  --gcs-prefix exports/daily

# Output: gs://first-bucket-practice-for-data-engineer/exports/daily/export_20240124_143052.jsonl

# BƯỚC 2: Cloud Function tự động trigger khi file upload xong

# BƯỚC 3: Kiểm tra logs
gcloud functions logs read bq-auto-loader --region=us-central1 --limit=20

# BƯỚC 4: Verify data trong BigQuery
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`consummate-rig-466909-i6.my_raw_dataset.events2\`"
```

---

## 📊 PHẦN 6: MONITORING & DEBUGGING

### Xem function status

```powershell
# Chi tiết function
gcloud functions describe bq-auto-loader --region=us-central1

# List tất cả functions
gcloud functions list --region=us-central1
```

### Xem BigQuery jobs

```powershell
# List jobs
bq ls -j --max_results=10 consummate-rig-466909-i6

# Xem job detail
bq show -j JOB_ID

# Cancel job
bq cancel -j JOB_ID
```

### Web Console

```
https://console.cloud.google.com/functions/details/us-central1/bq-auto-loader?project=consummate-rig-466909-i6
```

---

## 🔧 PHẦN 7: TROUBLESHOOTING

### 1. Function không trigger

**Kiểm tra:**

```powershell
# File pattern đúng chưa?
# Phải là: exports/daily/*.jsonl
# Phải cùng bucket: first-bucket-practice-for-data-engineer

# Upload lại để test
gsutil cp test.jsonl gs://first-bucket-practice-for-data-engineer/exports/daily/test.jsonl
```

### 2. Permission errors

```powershell
# Kiểm tra service account
gcloud functions describe bq-auto-loader --region=us-central1 --gen2

# Cấp thêm quyền
PROJECT_ID="consummate-rig-466909-i6"
SERVICE_ACCOUNT="bq-auto-loader@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID `
  --member="serviceAccount:$SERVICE_ACCOUNT" `
  --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID `
  --member="serviceAccount:$SERVICE_ACCOUNT" `
  --role="roles/storage.objectViewer"
```

### 3. Timeout errors

**Giải pháp:**

```powershell
# Tăng timeout và memory
gcloud functions deploy bq-auto-loader `
  --timeout=1800s `
  --memory=2GB `
  --region=us-central1
```

### 4. Schema errors

```powershell
# Kiểm tra schema
cat bq\cloud_functions\bq_auto_loader\schema.json

# Validate schema
bq show --schema --format=prettyjson consummate-rig-466909-i6:my_raw_dataset.events2
```

---

## 📝 PHẦN 8: UPDATE FUNCTION

### Update code

```powershell
cd bq\cloud_functions\bq_auto_loader

# Chỉ update code
gcloud functions deploy bq-auto-loader --source . --region=us-central1
```

### Update environment variables

```powershell
gcloud functions deploy bq-auto-loader `
  --update-env-vars TABLE=new_table,MAX_BAD_RECORDS=2000 `
  --region=us-central1
```

### Delete function

```powershell
gcloud functions delete bq-auto-loader --region=us-central1
```

---

## ⚙️ PHẦN 9: CẤU HÌNH NÂNG CAO

### Environment Variables:

| Variable                | Description                                     | Default                    |
| ----------------------- | ----------------------------------------------- | -------------------------- |
| `PROJECT_ID`            | GCP Project ID                                  | `consummate-rig-466909-i6` |
| `DATASET`               | BigQuery dataset                                | `my_raw_dataset`           |
| `TABLE`                 | BigQuery table                                  | `events2`                  |
| `WRITE_DISPOSITION`     | `WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY` | `WRITE_APPEND`             |
| `MAX_BAD_RECORDS`       | Số records bad tối đa                           | `1000`                     |
| `IGNORE_UNKNOWN_VALUES` | Ignore fields không có trong schema             | `true`                     |
| `SCHEMA_PATH`           | Đường dẫn schema                                | `/workspace/schema.json`   |

### File Patterns:

- ✅ **Extensions:** `.jsonl`, `.json`
- ✅ **Path:** `exports/daily/*.jsonl`
- ❌ **Excludes:** `_fixed`, `_chunk_`, `_temp`

---

## 📚 PHẦN 10: TÀI LIỆU THAM KHẢO

### Useful Commands:

```powershell
# Export từ MongoDB
python export_to_gcs.py --db db --collection collection --query-file query.json --format jsonl --gcs-bucket bucket

# Deploy Cloud Function
cd bq\cloud_functions\bq_auto_loader
.\deploy.sh  # hoặc deploy thủ công

# Upload test file
gsutil cp test.jsonl gs://bucket/exports/daily/test.jsonl

# Xem logs
gcloud functions logs read bq-auto-loader --region=us-central1 --limit=20

# Kiểm tra BigQuery
bq query "SELECT COUNT(*) FROM \`project.dataset.table\`"
```

### Links:

- Cloud Functions Docs: https://cloud.google.com/functions/docs
- BigQuery Docs: https://cloud.google.com/bigquery/docs
- GCS Triggers: https://cloud.google.com/functions/docs/tutorials/storage

---

## ✅ CHECKLIST DEPLOY

- [ ] Setup Google Cloud authentication
- [ ] Bật các APIs cần thiết
- [ ] Install dependencies
- [ ] Copy schema file vào bq_auto_loader
- [ ] Deploy Cloud Function
- [ ] Test với file upload
- [ ] Check logs
- [ ] Verify data trong BigQuery

---

## 🎉 DONE!

Sau khi hoàn thành tất cả các bước trên, bạn sẽ có:

1. ✅ Cloud Function `bq-auto-loader` đã deploy
2. ✅ Tự động load file từ GCS → BigQuery
3. ✅ Monitoring và logging đầy đủ
4. ✅ Pipeline hoàn chỉnh: MongoDB → GCS → BigQuery
