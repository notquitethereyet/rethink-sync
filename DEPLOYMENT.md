# Rethink BH Sync - Cloud Run Deployment Guide

This guide covers deploying the Rethink BH to Supabase sync service to Google Cloud Run.

## 📋 Prerequisites

1. **Google Cloud Project** with billing enabled
2. **gcloud CLI** installed and authenticated
3. **Docker** (optional, for local testing)
4. **Rethink BH credentials** (email/password)
5. **Supabase database URL**

## 🚀 Quick Deployment

### 1. Clone and Setup

```bash
# Navigate to project directory
cd /path/to/rethink-automation

# Make deployment scripts executable
chmod +x deploy.sh setup-secrets.sh
```

### 2. Deploy to Cloud Run

```bash
# Deploy with your project ID and preferred region
./deploy.sh YOUR_PROJECT_ID us-central1
```

This script will:
- Enable required Google Cloud APIs
- Build and push the container image
- Deploy to Cloud Run with optimal settings
- Output the service URL

### 3. Configure Secrets

```bash
# Set up Secret Manager secrets
./setup-secrets.sh YOUR_PROJECT_ID

# Add your actual secret values
gcloud secrets versions add RTHINK_USER --data-file=<(echo 'your-email@example.com')
gcloud secrets versions add RTHINK_PASS --data-file=<(echo 'your-password')
gcloud secrets versions add SUPABASE_DB_URL --data-file=<(echo 'postgresql://user:pass@host:port/db')
```

### 4. Test the Deployment

```bash
# Get your service URL from the deployment output, then test:
curl https://your-service-url.run.app/health
curl https://your-service-url.run.app/run
```

## 🔧 Manual Deployment Steps

If you prefer manual deployment:

### 1. Enable APIs

```bash
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable secretmanager.googleapis.com
```

### 2. Build Container

```bash
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/rethink-sync
```

### 3. Deploy Service

```bash
gcloud run deploy rethink-sync \
  --image gcr.io/YOUR_PROJECT_ID/rethink-sync \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi \
  --cpu 1 \
  --timeout 300 \
  --max-instances 10 \
  --set-env-vars GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
```

## 🔐 Security Configuration

### Optional API Authentication

To add simple API key authentication:

```bash
# Create an API auth key secret
gcloud secrets create API_AUTH_KEY --data-file=<(echo 'your-secret-key')

# Update the Cloud Run service to use the secret
gcloud run services update rethink-sync \
  --region us-central1 \
  --set-secrets API_AUTH_KEY=API_AUTH_KEY:latest
```

Then include the key in requests:
```bash
# As query parameter
curl "https://your-service-url.run.app/run?auth_key=your-secret-key"

# As header
curl -H "X-Auth-Key: your-secret-key" https://your-service-url.run.app/run
```

## 📊 Monitoring and Logs

### View Logs

```bash
# Real-time logs
gcloud run services logs tail rethink-sync --region us-central1

# Recent logs
gcloud run services logs read rethink-sync --region us-central1 --limit 50
```

### Cloud Monitoring

The service includes:
- Health check endpoint: `/health`
- Structured logging for Cloud Logging
- Error tracking and alerting

## 🔄 Make.com Integration

### Webhook Setup

1. In Make.com, create a new scenario
2. Add "HTTP Request" module
3. Configure:
   - **URL**: `https://your-service-url.run.app/run`
   - **Method**: GET or POST
   - **Headers**: `X-Auth-Key: your-secret-key` (if auth enabled)

### Scheduling

Set up the scenario to run daily:
- **Schedule**: Every day at 3:00 AM
- **Timezone**: Your preferred timezone

## 🛠️ Local Development

### Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export RTHINK_USER="your-email"
export RTHINK_PASS="your-password"
export SUPABASE_DB_URL="your-db-url"

# Run the server
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

### Test Locally

```bash
curl http://localhost:8080/health
curl http://localhost:8080/run
```

### Docker Testing

```bash
# Build image
docker build -t rethink-sync .

# Run container
docker run -p 8080:8080 \
  -e RTHINK_USER="your-email" \
  -e RTHINK_PASS="your-password" \
  -e SUPABASE_DB_URL="your-db-url" \
  rethink-sync
```

## 📋 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Service information |
| `/health` | GET | Comprehensive health check |
| `/ready` | GET | Readiness check |
| `/run` | POST | Execute sync |
| `/docs` | GET | API documentation |
| `/redoc` | GET | Alternative API docs |

## 🔍 Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify Rethink BH credentials in Secret Manager
   - Check secret permissions

2. **Database Connection Failed**
   - Verify Supabase URL format
   - Check database accessibility

3. **Timeout Errors**
   - Increase Cloud Run timeout (max 3600s)
   - Check network connectivity

4. **Memory Issues**
   - Increase Cloud Run memory allocation
   - Monitor memory usage in logs

### Debug Commands

```bash
# Check service status
gcloud run services describe rethink-sync --region us-central1

# View recent deployments
gcloud run revisions list --service rethink-sync --region us-central1

# Check secrets
gcloud secrets versions list RTHINK_USER
```

## 📈 Performance Optimization

### Recommended Settings

- **Memory**: 1Gi (can reduce to 512Mi for smaller datasets)
- **CPU**: 1 (sufficient for most workloads)
- **Timeout**: 300s (increase if processing large datasets)
- **Max Instances**: 10 (adjust based on usage)

### Cost Optimization

- Use minimum necessary resources
- Set appropriate max instances
- Consider using Cloud Scheduler instead of Make.com for simple scheduling

## 🔄 Updates and Maintenance

### Deploy Updates

```bash
# Redeploy with latest code
./deploy.sh YOUR_PROJECT_ID

# Or manually
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/rethink-sync
gcloud run services update rethink-sync --image gcr.io/YOUR_PROJECT_ID/rethink-sync --region us-central1
```

### Backup and Recovery

- Secrets are automatically backed up in Secret Manager
- Container images are stored in Container Registry
- Consider backing up your Supabase database regularly
