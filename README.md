# 🏥 Rethink BH Automation

Production-ready automated data sync from Rethink Behavioral Health to Supabase database, deployable to Google Cloud Run with comprehensive logging and monitoring.

## 🎯 Overview

This tool automates the download of appointment data, cancelled appointments, and Over Term dashboard data from Rethink BH and ingests it into your Supabase PostgreSQL database. Features enterprise-grade logging, security monitoring, rate limiting, and health checks for production deployment on Google Cloud Run.

> **🔒 Security Update**: All main endpoints (`/run`, `/overterm-dashboard`, `/overterm-sync`, `/cancelled-appointments-sync`) are now POST-only for enhanced security. GET endpoints have been removed.

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
   curl -X POST https://your-service-url.run.app/run \
     -H "Content-Type: application/json" \
     -d '{"from_date":"2024-01-01","to_date":"2024-01-31","table_name":"rethinkdump"}'
   ```

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

### 💻 Local Development

#### Prerequisites
- Python 3.11+
- Package manager: `pip` or `uv` (recommended for faster installs)
- Supabase database access

#### Setup
1. Install dependencies:
   ```bash
   # Using pip
   pip install -r requirements.txt

   # Or using uv (faster)
   uv pip install -r requirements.txt
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
# Appointment data sync
curl -X POST https://your-service-url.run.app/run \
  -H "Content-Type: application/json" \
  -d '{"from_date":"2024-01-01","to_date":"2024-01-31","table_name":"rethinkdump"}'

# Over Term dashboard sync
curl -X POST https://your-service-url.run.app/overterm-sync \
  -H "Content-Type: application/json" \
  -d '{"table_name":"overterm_dashboard"}'

# Cancelled appointments sync
curl -X POST https://your-service-url.run.app/cancelled-appointments-sync \
  -H "Content-Type: application/json" \
  -d '{"from_date":"2024-01-01","to_date":"2024-01-31","table_name":"cancelled_appointments","truncate":true}'
```

### Local Development & Testing
For local development and testing:
```bash
# Install dependencies
pip install -r requirements.txt
# Or: uv pip install -r requirements.txt

# Run the FastAPI server locally
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
# Or: uv run uvicorn main:app --host 0.0.0.0 --port 8080 --reload

# Test the endpoints
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"from_date":"2024-01-01","to_date":"2024-01-31","table_name":"rethinkdump"}'
```

## 📁 Files

### Core Application
- `main.py` - **FastAPI application** - Production-ready webhook service with comprehensive logging
- `rethink_sync.py` - **Appointment sync module** - Downloads and syncs appointment data
- `overterm_dashboard.py` - **Over Term dashboard module** - Syncs Over Term authorization data
- `cancelled_appointments.py` - **Cancelled appointments module** - Syncs cancelled appointments data
- `auth.py` - **Authentication module** - Handles Rethink BH login and session management

### Deployment & Configuration
- `Dockerfile` - Production container configuration for Google Cloud Run
- `requirements.txt` - Python dependencies
- `pyproject.toml` - Project configuration
- `deploy.sh` - Automated deployment script
- `setup-secrets.sh` - Secret Manager configuration script
- `DEPLOYMENT.md` - Detailed deployment guide



## 🔧 Features

### Production-Ready Service
- **Enterprise logging** - Structured logging with request tracing, performance metrics, and security events
- **Security monitoring** - Rate limiting, authentication logging, and suspicious activity detection
- **Health & monitoring** - Comprehensive health checks with dependency validation
- **Google Cloud integration** - Secret Manager, Cloud Logging, and Cloud Run optimized
- **Auto-scaling** - Handles variable workloads with performance monitoring
- **Error handling** - Comprehensive exception handling with context logging

### Data Sync Capabilities
- **Appointment data sync** - Downloads and syncs appointment data from Rethink BH
- **Over Term dashboard** - Syncs Over Term authorization utilization data
- **Dual table support** - Separate tables for appointments (`rethinkdump`) and Over Term data (`overterm_dashboard`)
- **Memory-efficient** - In-memory processing without file I/O
- **Batch processing** - Optimized database inserts for high performance
- **Data integrity** - Truncate and reload for consistent data state

### Authentication & Security
- **Session management** - Handles Rethink BH authentication with token management
- **Rate limiting** - Configurable request rate limiting with security logging
- **API authentication** - Optional API key authentication for webhook endpoints
- **Credential security** - Google Secret Manager integration with environment fallback
- **Security logging** - Comprehensive audit trail for authentication and access events

### Monitoring & Observability
- **Request tracing** - Unique request IDs with full request lifecycle logging
- **Performance metrics** - Response times, throughput, and slow request detection
- **Error tracking** - Detailed error logging with stack traces and context
- **Health endpoints** - Multiple health check levels for different monitoring needs

## 📋 Database Schema

### Appointment Data (`rethinkdump` table)
- Auto-incrementing primary key (`id`)
- All appointment fields (type, location, client, staff, etc.)
- Supports duplicate `appointmentID`s for recurring series
- Truncated and reloaded on each sync for data consistency

### Over Term Dashboard (`overterm_dashboard` table)
- Auto-incrementing primary key (`id`)
- Authorization utilization details for Over Term clients
- Client information, authorization details, and utilization metrics
- Separate sync process with dedicated endpoint

## 🌐 API Endpoints (Cloud Run)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service information and status |
| `/health` | GET | Comprehensive health check with dependency validation |
| `/ready` | GET | Kubernetes-style readiness check |
| `/run` | POST | Execute appointment data sync |
| `/overterm-dashboard` | POST | Fetch Over Term dashboard data (read-only) |
| `/overterm-sync` | POST | Execute Over Term dashboard sync to database |
| `/docs` | GET | Interactive API documentation |
| `/redoc` | GET | Alternative API documentation |

### Authentication
- Optional API key authentication via `auth_key` in JSON body or `X-Auth-Key` header
- Set `API_AUTH_KEY` environment variable to enable authentication
- Rate limiting: 60 requests per minute per IP address
- **Note**: All main endpoints (`/run`, `/overterm-dashboard`, `/overterm-sync`) are POST-only for security

## 🔗 Integration

### Make.com Webhook
1. **Appointment Sync**: Create HTTP Request module with URL `https://your-service-url.run.app/run`
2. **Over Term Sync**: Create HTTP Request module with URL `https://your-service-url.run.app/overterm-sync`
3. **Method**: POST (required)
4. **Headers**: `Content-Type: application/json`
5. **Body**: JSON with required parameters (see examples below)
6. **Schedule**: Daily at preferred time
7. **Authentication**: Add `auth_key` to JSON body if `API_AUTH_KEY` is configured

#### Example JSON Bodies:
```json
// Appointment sync
{
  "from_date": "2024-01-01",
  "to_date": "2024-01-31",
  "table_name": "rethinkdump"
}

// Over Term sync
{
  "table_name": "overterm_dashboard"
}
```

### Google Cloud Scheduler (Alternative)
```bash
# Appointment data sync
gcloud scheduler jobs create http rethink-sync-daily \
  --schedule "0 3 * * *" \
  --uri https://your-service-url.run.app/run \
  --http-method POST \
  --headers "Content-Type=application/json" \
  --message-body '{"from_date":"2024-01-01","to_date":"2024-06-30","table_name":"rethinkdump"}'

# Over Term dashboard sync
gcloud scheduler jobs create http overterm-sync-daily \
  --schedule "0 4 * * *" \
  --uri https://your-service-url.run.app/overterm-sync \
  --http-method POST \
  --headers "Content-Type=application/json" \
  --message-body '{"table_name":"overterm_dashboard"}'
```

## 📊 Logging & Monitoring

### Production Logging
- **Structured logging** with consistent format and request tracing
- **Log levels**: DEBUG, INFO, WARNING, ERROR with configurable `LOG_LEVEL` environment variable
- **Request lifecycle**: Full request/response logging with timing and performance metrics
- **Security events**: Authentication failures, rate limiting, and suspicious activity
- **Error tracking**: Comprehensive error logging with stack traces and context

### Google Cloud Logging Integration
- Automatic log aggregation in Google Cloud Console
- Searchable logs with structured fields and metadata
- Alert policies can be configured on log patterns
- Performance and error metrics available in Cloud Monitoring

### Key Log Patterns
- `REQUEST_START/COMPLETE` - Request lifecycle with timing
- `AUTH_SUCCESS/FAILURE` - Authentication events
- `SYNC_START/COMPLETE/FAILED` - Data sync operations
- `RATE_LIMIT_EXCEEDED` - Rate limiting events
- `SLOW_REQUEST/SYNC` - Performance warnings

### Environment Variables
- `LOG_LEVEL` - Set logging level (DEBUG, INFO, WARNING, ERROR)
- `API_AUTH_KEY` - Optional API authentication key
- `PORT` - Server port (default: 8080)

## 📚 Documentation

- [DEPLOYMENT.md](DEPLOYMENT.md) - Complete deployment guide
- [PRD.md](PRD.md) - Project requirements document
- API docs available at `/docs` endpoint when deployed

## 📝 Recent Changes

### v1.4.0 - Enhanced Cancelled Appointments Sync
- **🔄 Complete Schema Support** - Full field mapping from Rethink API to `cancelled_dump` table
- **🧩 Schema Alignment** - All fields required by `cancelledAppointments` schema are now captured
- **⏰ UI Timestamp Support** - Added support for UI-style date format (e.g., '8/31/2025, 3:00:00 AM')
- **🔄 ISO Date Conversion** - Automatic conversion of UI dates to ISO format for API requests
- **📊 Duration Calculation** - Added calculated duration field combining hours and minutes
- **🏷️ Enhanced Metadata** - Additional fields like series_appointment_id, parent_verification, and more
- **📝 Documentation** - Updated schema documentation and SQL migration scripts

### v1.3.0 - Cancelled Appointments Sync
- **🗓️ New Endpoint** - Added `/cancelled-appointments-sync` endpoint for syncing cancelled appointments
- **🔄 Full Workflow** - Fetches cancelled appointments from Rethink BH and syncs to database
- **🧹 Table Management** - Supports table truncation before insertion for clean data
- **📊 Detailed Results** - Returns comprehensive sync statistics and timing information
- **✅ Validation** - Strict request validation with Pydantic models
- **📝 Documentation** - Enhanced FastAPI documentation with request/response examples

### v1.2.0 - Client Name Anonymization
- **🔐 Privacy Enhancement** - Client names are now anonymized using nameCode in the run endpoint
- **🏷️ nameCode Format** - 4-character code using first two letters of first name + first two of last name (e.g., "John Doe" → "JoDo")
- **🧩 Format Handling** - Properly handles different name formats and ignores nicknames in parentheses
- **🔄 Consistent Anonymization** - Same person will always generate the same nameCode
- **🛡️ Data Protection** - Full client names are no longer stored in the database for the run endpoint

### v1.1.0 - Security Enhancement
- **🔒 Removed GET endpoints** for `/run`, `/overterm-dashboard`, and `/overterm-sync`
- **✅ POST-only API** - All main endpoints now require POST requests with JSON body
- **🛡️ Enhanced security** - Prevents accidental data exposure via URL parameters
- **📖 Updated documentation** - All examples now show proper POST requests with JSON payloads
- **🔧 Maintained compatibility** - POST endpoints retain all functionality from previous GET endpoints

### Migration Guide
If you were using GET requests, update your calls:
```bash
# Old (removed)
curl "https://your-service.run.app/run?from_date=2024-01-01&to_date=2024-01-31&table_name=rethinkdump"

# New (required)
curl -X POST https://your-service.run.app/run \
  -H "Content-Type: application/json" \
  -d '{"from_date":"2024-01-01","to_date":"2024-01-31","table_name":"rethinkdump"}'
```

