Here’s a **detailed project requirement document** for converting your Rethink → Supabase pipeline into a Cloud Run-hosted webhook, callable via Make.com
---

## 📄 Project Requirements Document

**Project Name:**
Daily Rethink → Supabase Data Sync via Google Cloud Run

**Prepared for:**
AllCheer Tools

**Date:**
June 21, 2025

---

### ✅ Project Objective

To automate the daily download of appointment data from the Rethink Behavioral Health dashboard and ingest it into a Supabase PostgreSQL table (`rethinkDump`). The goal is to host this logic as a single HTTP endpoint on **Google Cloud** that can be triggered daily via **Make.com**.

---

### 🔧 Functional Requirements

#### 1. **Python Script Unification**

* ✅ **COMPLETED** - Combined download and ingestion logic into a single unified script.
* The unified script should:

  1. ✅ Authenticate and download the latest Excel data from Rethink (in-memory processing).
  2. ✅ Truncate and reset the Supabase table (`rethinkDump`).
  3. ✅ Map and insert all Excel rows into Supabase using optimized batch processing.
  4. ✅ Memory-efficient processing without temporary file storage.

#### 2. **API Wrapper**

* ✅ **COMPLETED** - Wrapped the unified script in a **FastAPI** app.
* ✅ **COMPLETED** - Created multiple endpoints:

  * `GET/POST /run` → Executes the download + ingestion logic.
  * `GET /health` → Health check endpoint.
  * `GET /` → Service information.
  * `GET /docs` → API documentation.
* ✅ **COMPLETED** - Enhanced JSON status report with timing information:

  ```json
  {
    "status": "success",
    "rows_inserted": 3894,
    "errors": 0,
    "total_rows": 3894,
    "timestamp": "2025-06-21T17:32:20.577612",
    "duration_seconds": 23.7,
    "message": "Sync completed successfully"
  }
  ```

#### 3. **Cloud Hosting with Google Cloud Run**

* Dockerize the FastAPI app.
* Deploy the container to **Google Cloud Run**:

  * Runtime: Python 3.11
  * Trigger: HTTP (allow unauthenticated access unless security is required)
  * Timeout: 300 seconds
  * Region: `us-central1` or preferred

#### 4. **Triggering Options**

Provide support to trigger the webhook:

##### a. Make.com (Webhook HTTP Request Module)

* Scenario: Custom automation using Make
* Trigger: HTTP GET to Cloud Run endpoint
* Optional: Add Make webhook key as a query param for basic security

#### 5. **Environment Variables (via .env or Secret Manager)**

The following variables must be securely loaded:

* `RTHINK_USER`
* `RTHINK_PASS`
* `SUPABASE_DB_URL`

---

### 📦 Non-Functional Requirements

| Requirement    | Description                                                                          |
| -------------- | ------------------------------------------------------------------------------------ |
| Security       | No PII should be printed in logs. Optionally restrict access via Auth Header or OIDC |
| Logging        | Include `print()` logs or structured logging for cloud monitoring                    |
| Error Handling | Must not partially truncate/upload; fail gracefully if download fails                |
| Scalability    | Should handle up to 5,000 rows per execution                                         |
| Reliability    | Retry logic not required        |

---

### 📁 Project Structure

```
project-root/
│
├── main.py             # FastAPI entrypoint
├── rethink_sync.py     # Contains all core logic (download + upload)
├── requirements.txt
├── Dockerfile
└── .env                # Local-only (not committed)
```

---

### 🐳 Dockerfile Requirements

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

---

### 📡 API Endpoint Summary

| Endpoint | Method | Description                           | Security                                  |
| -------- | ------ | ------------------------------------- | ----------------------------------------- |
| `/run`   | GET    | Run full sync job (download + upload) | Optional query key or Cloud Scheduler IAM |

---

### 🚀 Deployment Steps

1. **Build Docker image:**

   ```bash
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/rethink-sync
   ```

2. **Deploy to Cloud Run:**

   ```bash
   gcloud run deploy rethink-sync \
     --image gcr.io/YOUR_PROJECT_ID/rethink-sync \
     --platform managed \
     --region us-central1 \
     --allow-unauthenticated
   ```

3. **Set up Cloud Scheduler (optional):**

   ```bash
   gcloud scheduler jobs create http rethink-sync-daily \
     --schedule "0 3 * * *" \
     --uri https://rethink-sync-xxxx.a.run.app/run \
     --http-method GET
   ```

---

### ✅ Acceptance Criteria

| Criterion                            | Description                                                 |
| ------------------------------------ | ----------------------------------------------------------- |
| API responds with success            | `/run` endpoint returns JSON with rows inserted             |
| Excel is downloaded correctly        | File saved in memory or tempdir and parsed                  |
| Supabase table is truncated          | Old data is deleted, IDs are reset                          |
| Data is inserted without error       | All rows accounted for (log errors if any)                  |
| File is deleted after upload         | Prevents duplication on future runs                         |
| Endpoint can be triggered externally | Works from Make.com or Cloud Scheduler without auth headers |

---

Would you like me to prepare the actual `main.py`, `Dockerfile`, and `requirements.txt` to match this plan?
