#!/usr/bin/env python3
"""
FastAPI application for Rethink BH to Supabase sync.
Designed for Google Cloud Run deployment with webhook support.
"""

import os
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
import time
from dotenv import load_dotenv
from rethink_sync import RethinkSync, RethinkSyncError
from auth import RethinkAuth, RethinkAuthError
from overterm_dashboard import OverTermDashboard, OverTermDashboardError

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Rethink BH Sync API",
    description="Automated sync service for Rethink Behavioral Health to Supabase",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Add rate limiting
RATE_LIMIT = 60  # requests per minute
RATE_LIMIT_WINDOW = 60  # seconds
client_requests = {}

def rate_limit(request: Request):
    client_ip = request.client.host
    current_time = time.time()
    
    if client_ip not in client_requests:
        client_requests[client_ip] = []
    
    # Clean up old requests
    client_requests[client_ip] = [t for t in client_requests[client_ip] if t > current_time - RATE_LIMIT_WINDOW]
    
    if len(client_requests[client_ip]) >= RATE_LIMIT:
        raise HTTPException(
            status_code=HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Please wait before making another request."
        )
    
    client_requests[client_ip].append(current_time)
    return True

# Optional simple authorization
AUTH_KEY = os.getenv("API_AUTH_KEY")

def check_auth(request: Request) -> bool:
    """Simple authorization check if AUTH_KEY is set."""
    if not AUTH_KEY:
        return True  # No auth required if key not set
    
    # Check for auth key in query params or headers
    auth_from_query = request.query_params.get("auth_key")
    auth_from_header = request.headers.get("X-Auth-Key")
    
    if not (auth_from_query == AUTH_KEY or auth_from_header == AUTH_KEY):
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials"
        )
    return True

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Log request details
    logger.info(f"Request received: {request.method} {request.url.path}")
    logger.debug(f"Headers: {dict(request.headers)}")
    
    response = await call_next(request)
    
    # Log response details
    process_time = time.time() - start_time
    logger.info(f"Request completed in {process_time:.4f} seconds")
    logger.debug(f"Status code: {response.status_code}")
    
    return response

@app.exception_handler(RethinkSyncError)
async def rethink_sync_exception_handler(request: Request, exc: RethinkSyncError):
    """Handle RethinkSyncError exceptions."""
    logger.error(f"Sync error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(RethinkAuthError)
async def rethink_auth_exception_handler(request: Request, exc: RethinkAuthError):
    """Handle RethinkAuthError exceptions."""
    logger.error(f"Authentication error: {str(exc)}")
    return JSONResponse(
        status_code=401,
        content={
            "status": "error",
            "message": f"Authentication failed: {str(exc)}",
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(OverTermDashboardError)
async def overterm_dashboard_exception_handler(request: Request, exc: OverTermDashboardError):
    """Handle OverTermDashboardError exceptions."""
    logger.error(f"Dashboard error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unexpected error: {str(exc)}")
    logger.error(traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error",
            "timestamp": datetime.now().isoformat()
        }
    )

@app.get("/")
async def root():
    """Root endpoint with basic service information."""
    return {
        "service": "Rethink BH Sync API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "sync": "POST /run",
            "overterm-dashboard": "POST /overterm-dashboard",
            "overterm-sync": "POST /overterm-sync",
            "health": "GET /health",
            "ready": "GET /ready",
            "docs": "GET /docs"
        }
    }

@app.get("/health")
async def health_check():
    """
    Comprehensive health check endpoint for Cloud Run and monitoring.

    Checks:
    - Service availability
    - Environment configuration
    - Critical dependencies
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "rethink-sync",
        "version": "1.0.0"
    }

    # Check environment configuration
    checks = {
        "environment": "pass",
        "secrets": "pass",
        "dependencies": "pass"
    }

    # Check if we can access environment/secrets
    try:
        from rethink_sync import RethinkSync
        sync_service = RethinkSync()
        # Try to get credentials (this will test Secret Manager access)
        sync_service._get_credentials()
        checks["secrets"] = "pass"
    except Exception as e:
        checks["secrets"] = "fail"
        health_status["status"] = "degraded"
        logger.warning(f"Health check - secrets access failed: {str(e)}")

    # Check critical imports
    try:
        import pandas
        import psycopg2
        import requests
        checks["dependencies"] = "pass"
    except ImportError as e:
        checks["dependencies"] = "fail"
        health_status["status"] = "unhealthy"
        logger.error(f"Health check - dependency missing: {str(e)}")

    health_status["checks"] = checks

    # Return appropriate HTTP status
    if health_status["status"] == "unhealthy":
        return JSONResponse(status_code=503, content=health_status)
    elif health_status["status"] == "degraded":
        return JSONResponse(status_code=200, content=health_status)
    else:
        return health_status

@app.get("/ready")
async def readiness_check():
    """
    Kubernetes-style readiness check.
    Simple endpoint that returns 200 if the service is ready to accept traffic.
    """
    return {"status": "ready", "timestamp": datetime.now().isoformat()}



from pydantic import BaseModel

class SyncRequest(BaseModel):
    from_date: Optional[str] = None
    to_date: Optional[str] = None
    table_name: Optional[str] = 'rethinkdump'
    auth_key: Optional[str] = None

@app.post("/run")
async def run_sync_post(request: Request) -> Dict[str, Any]:
    """
    POST version of the sync endpoint for webhook compatibility.
    
    Accepts JSON body with required parameters:
    - from_date: Start date in YYYY-MM-DD format
    - to_date: End date in YYYY-MM-DD format
    - table_name: Name of the table to insert data into
    - auth_key: Optional API key for authentication
    
    Returns the same response as the GET endpoint or error response for malformed requests.
    """
    try:
        # Parse JSON body
        body = await request.json()
        
        # Check for required fields
        if 'from_date' not in body:
            logger.error("Malformed request: Missing required field 'from_date'")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Missing required field: from_date",
                    "timestamp": datetime.now().isoformat()
                }
            )
            
        if 'to_date' not in body:
            logger.error("Malformed request: Missing required field 'to_date'")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Missing required field: to_date",
                    "timestamp": datetime.now().isoformat()
                }
            )
            
        if 'table_name' not in body:
            logger.error("Malformed request: Missing required field 'table_name'")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "Missing required field: table_name",
                    "timestamp": datetime.now().isoformat()
                }
            )
        
        from_date = body['from_date']
        to_date = body['to_date']
        table_name = body['table_name']
        
        # If auth_key is in the body, add it to the query params for the GET handler
        if 'auth_key' in body:
            request.scope['query_string'] = f"auth_key={body['auth_key']}"
            
    except json.JSONDecodeError as e:
        # Invalid JSON format
        logger.error(f"Malformed request: Invalid JSON format - {e}")
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "Invalid JSON format in request body",
                "timestamp": datetime.now().isoformat()
            }
        )
    except Exception as e:
        # Other unexpected errors
        logger.error(f"Error processing request body: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Error processing request: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }
        )

    # Check authorization if enabled
    if not check_auth(request):
        logger.warning("Unauthorized sync attempt")
        raise HTTPException(
            status_code=401,
            detail="Unauthorized: Invalid or missing auth key"
        )

    start_time = datetime.now()
    logger.info(f"Sync request received at {start_time.isoformat()}")

    try:
        # Initialize sync service
        sync_service = RethinkSync()

        # Execute sync with parameters
        result = sync_service.run_sync(
            from_date=from_date,
            to_date=to_date,
            table_name=table_name
        )

        # Add timing information
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        response = {
            **result,
            "timestamp": end_time.isoformat(),
            "duration_seconds": round(duration, 2),
            "message": "Sync completed successfully"
        }

        logger.info(f"Sync completed successfully in {duration:.2f}s: {result['rows_inserted']} rows inserted")
        return response

    except RethinkSyncError as e:
        # Log specific sync errors with context
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Sync failed after {duration:.2f}s: {str(e)}")
        # Re-raise to be handled by exception handler
        raise e
    except Exception as e:
        # Log and re-raise unexpected errors with full context
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Unexpected error in sync endpoint after {duration:.2f}s: {str(e)}")
        logger.error(traceback.format_exc())
        raise e



class DashboardRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    client_ids: Optional[list[int]] = None  # List of client IDs
    auth_key: Optional[str] = None

@app.post("/overterm-dashboard")
async def get_overterm_dashboard_post(request: Request) -> Dict[str, Any]:
    """POST version of the overterm-dashboard endpoint."""
    try:
        body = await request.json()

        # Extract parameters
        start_date = body.get('start_date')
        end_date = body.get('end_date')
        client_ids = body.get('client_ids')

        # Handle auth_key in body
        if 'auth_key' in body:
            request.scope['query_string'] = f"auth_key={body['auth_key']}"

        # Check authorization if enabled
        if not check_auth(request):
            logger.warning("Unauthorized dashboard request")
            raise HTTPException(
                status_code=401,
                detail="Unauthorized: Invalid or missing auth key"
            )

        # Apply rate limiting
        rate_limit(request)

        start_time = datetime.now()
        logger.info("Over Term dashboard request received")

        try:
            # Fetch dashboard data
            dashboard_service = OverTermDashboard()
            result = dashboard_service.get_dashboard_data(
                start_date=start_date,
                end_date=end_date,
                client_ids=client_ids
            )

            # Add timing
            duration = (datetime.now() - start_time).total_seconds()
            result["duration_seconds"] = round(duration, 2)

            logger.info(f"Dashboard request completed in {duration:.2f}s")
            return result

        except (RethinkAuthError, OverTermDashboardError) as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Dashboard request failed after {duration:.2f}s: {e}")
            raise
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Unexpected error after {duration:.2f}s: {e}")
            raise

    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        logger.error(f"Error processing request body: {e}")
        raise HTTPException(status_code=500, detail="Error processing request")



class OverTermSyncRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    client_ids: Optional[List[int]] = None
    table_name: Optional[str] = "overterm_dashboard"
    auth_key: Optional[str] = None

@app.post("/overterm-sync")
async def sync_overterm_dashboard_post(request: Request) -> Dict[str, Any]:
    """POST version of the overterm-sync endpoint."""
    try:
        body = await request.json()

        # Extract parameters
        start_date = body.get('start_date')
        end_date = body.get('end_date')
        client_ids = body.get('client_ids')
        table_name = body.get('table_name', 'overterm_dashboard')

        # Handle auth_key in body
        if 'auth_key' in body:
            request.scope['query_string'] = f"auth_key={body['auth_key']}"

        # Check authorization
        if not check_auth(request):
            logger.warning("Unauthorized Over Term sync request")
            raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing auth key")

        # Apply rate limiting
        rate_limit(request)

        start_time = datetime.now()
        logger.info("Over Term sync request received")

        try:
            # Perform sync
            dashboard_service = OverTermDashboard()
            result = dashboard_service.sync_to_database(
                start_date=start_date,
                end_date=end_date,
                client_ids=client_ids,
                table_name=table_name
            )

            # Add timing
            duration = (datetime.now() - start_time).total_seconds()
            result["duration_seconds"] = round(duration, 2)

            logger.info(f"Over Term sync completed in {duration:.2f}s: {result.get('records_inserted', 0)} records")
            return result

        except (RethinkAuthError, OverTermDashboardError) as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Over Term sync failed after {duration:.2f}s: {e}")
            raise
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Unexpected error in Over Term sync after {duration:.2f}s: {e}")
            raise

    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        logger.error(f"Error processing request body: {e}")
        raise HTTPException(status_code=500, detail="Error processing request")

# Cloud Run requires the app to listen on the PORT environment variable
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8080))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting server on {host}:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        log_level="info",
        access_log=True
    )
