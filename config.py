#!/usr/bin/env python3
"""
Configuration module for Rethink BH Sync application.
Centralizes all configuration constants and settings.
"""

import os
import logging
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Application configuration class."""
    
    # Application settings
    APP_NAME = "Rethink BH Sync API"
    APP_VERSION = "1.1.0"
    APP_DESCRIPTION = "Automated sync service for Rethink Behavioral Health to Supabase"
    
    # Server settings
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8080))
    
    # Logging settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Security settings
    API_AUTH_KEY = os.getenv("API_AUTH_KEY")
    
    # Rate limiting settings
    RATE_LIMIT_REQUESTS = 60  # requests per minute
    RATE_LIMIT_WINDOW = 60    # seconds
    
    # Rethink BH API settings
    RETHINK_BASE_URL = "https://webapp.rethinkbehavioralhealth.com"
    RETHINK_APPLICATION_KEY = "74569e11-18b4-4122-a58d-a4b830aa12c4"
    RETHINK_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:139.0) Gecko/139.0"
    
    # Google Cloud settings
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    
    # Request timeout settings
    REQUEST_TIMEOUT = 120  # seconds
    
    @classmethod
    def get_rethink_headers(cls) -> Dict[str, str]:
        """Get standard headers for Rethink BH API requests."""
        return {
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json, text/plain, */*",
            "X-Application-Key": cls.RETHINK_APPLICATION_KEY,
            "X-Origin": "Angular",
            "User-Agent": cls.RETHINK_USER_AGENT,
            "Origin": cls.RETHINK_BASE_URL,
            "Referer": f"{cls.RETHINK_BASE_URL}/Healthcare#/Login",
        }
    
    @classmethod
    def setup_logging(cls) -> logging.Logger:
        """Configure and return the application logger."""
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL),
            format=cls.LOG_FORMAT,
            handlers=[logging.StreamHandler()],
            force=True  # Override any existing configuration
        )
        return logging.getLogger(__name__)

class DatabaseConfig:
    """Database-specific configuration."""
    
    # Column mapping for Excel to database
    APPOINTMENT_COLUMN_MAPPING = {
        'Appointment Type': 'appointmentType',
        'Appointment Tag': 'appointmentTag', 
        'Service Line': 'serviceLine',
        'Service': 'service',
        'Appointment Location': 'appointmentLocation',
        'Duration': 'duration',
        'Day': 'day',
        'Date': 'date',
        'Time': 'time',
        'Client': 'client',
        'Client ID': 'clientId',
        'Staff': 'staff',
        'Staff ID': 'staffId',
        'Appointment ID': 'appointmentId',
        'Status': 'status',
        'Notes': 'notes',
        'Created Date': 'createdDate',
        'Modified Date': 'modifiedDate'
    }
    
    @classmethod
    def get_appointment_columns(cls) -> list:
        """Get list of database column names for appointments."""
        return list(cls.APPOINTMENT_COLUMN_MAPPING.values())

class HealthCheckConfig:
    """Health check configuration."""
    
    HEALTH_CHECK_TIMEOUT = 5  # seconds
    DEPENDENCY_CHECKS = [
        "pandas",
        "psycopg2", 
        "requests",
        "fastapi",
        "google.cloud.secretmanager"
    ]

# Create global config instance
config = Config()
db_config = DatabaseConfig()
health_config = HealthCheckConfig()
