# 🏥 Rethink BH Automation

Automated appointment data sync from Rethink Behavioral Health to Supabase database, deployable to Google Cloud Run for webhook automation.

## 🎯 Overview

This tool automates the daily download of appointment data from Rethink BH and ingests it into your Supabase PostgreSQL database. Available as both local scripts and a Cloud Run-hosted webhook service for integration with automation platforms like Make.com.

## ⚡ Quick Start

### 🚀 Cloud Run Deployment (Recommended)

1. **Deploy to Google Cloud:**
   ```bash
   ./deploy.sh YOUR_PROJECT_ID us-central1
   ./setup-secrets.sh YOUR_PROJECT_ID
   ```

2. **Configure secrets:**
   ```bash
   gcloud secrets versions add RTHINK_USER --data-file=<(echo 'your-email')
   gcloud secrets versions add RTHINK_PASS --data-file=<(echo 'your-password')
   gcloud secrets versions add SUPABASE_DB_URL --data-file=<(echo 'your-db-url')
   ```

3. **Test the webhook:**
   ```bash
   curl https://your-service-url.run.app/run
   ```

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

### 💻 Local Development

#### Prerequisites
- Python 3.11+
- pip package manager
- Supabase database access

#### Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```bash
   # Create .env file with your credentials
   echo "RTHINK_USER=your-email@example.com" > .env
   echo "RTHINK_PASS=your-password" >> .env
   echo "SUPABASE_DB_URL=postgresql://user:pass@host:port/db" >> .env
   ```

## 📊 Usage

### Cloud Run Service (Production)
The service runs automatically via webhook calls from Make.com or other automation platforms:
```bash
curl https://your-service-url.run.app/run
```

### Local Development & Testing
For local development and testing:
```bash
# Install dependencies
pip install -r requirements.txt

# Run the FastAPI server locally
uvicorn main:app --host 0.0.0.0 --port 8080 --reload

# Test the endpoint
curl http://localhost:8080/run
```

## 📁 Files

### Cloud Run Service
- `main.py` - **FastAPI application** - Webhook endpoint for Cloud Run
- `rethink_sync.py` - **Unified sync logic** - Combined download and ingestion
- `Dockerfile` - Container configuration for Cloud Run
- `requirements.txt` - Python dependencies for Docker
- `deploy.sh` - Automated deployment script
- `setup-secrets.sh` - Secret Manager configuration script
- `DEPLOYMENT.md` - Detailed deployment guide



## 🔧 Features

### Cloud Run Service
- **Webhook-ready API** - RESTful endpoint for automation platforms
- **Google Cloud Secret Manager** - Secure credential storage
- **Structured logging** - Cloud Logging integration
- **Health checks** - Built-in monitoring endpoints
- **Auto-scaling** - Handles variable workloads
- **Memory-efficient** - In-memory processing without file I/O
- **Batch processing** - Optimized database inserts for high performance

### Core Functionality
- **Browser-compliant downloads** - Matches exact browser behavior (~217 rows)
- **Daily table refresh** - Truncates and reloads data for up-to-date appointments
- **Sequential ID reset** - Auto-increment IDs reset to 1-N on each import
- **Handles recurring appointments** - Preserves all instances with duplicate appointmentIDs
- **Robust error handling** - Comprehensive logging and error recovery
- **6-month date range** - Automatically calculates optimal date range

### Local Development
- **FastAPI development server** - Local testing with hot reload
- **Environment variable support** - Fallback to .env for local development
- **Production-ready** - Same codebase for development and production

## 📋 Database Schema

The `rethinkDump` table includes:
- Auto-incrementing primary key (`id`)
- All appointment fields (type, location, client, staff, etc.)
- Supports duplicate `appointmentID`s for recurring series

## 🌐 API Endpoints (Cloud Run)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service information |
| `/health` | GET | Comprehensive health check |
| `/ready` | GET | Readiness check |
| `/run` | GET/POST | Execute sync |
| `/docs` | GET | API documentation |

## 🔗 Integration

### Make.com Webhook
1. Create HTTP Request module
2. Set URL to: `https://your-service-url.run.app/run`
3. Method: GET or POST
4. Schedule: Daily at preferred time

### Google Cloud Scheduler (Alternative)
```bash
gcloud scheduler jobs create http rethink-sync-daily \
  --schedule "0 3 * * *" \
  --uri https://your-service-url.run.app/run \
  --http-method GET
```

## 📚 Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment guide
- [PRD.md](PRD.md) - Project requirements document
- API docs available at `/docs` endpoint when deployed

