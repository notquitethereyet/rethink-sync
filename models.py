#!/usr/bin/env python3
"""
Pydantic models for request validation and response schemas.
Provides type safety and automatic validation for API endpoints.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import re

class SyncRequest(BaseModel):
    """Request model for appointment sync endpoint."""
    
    from_date: str = Field(
        ..., 
        description="Start date in YYYY-MM-DD format",
        example="2024-01-01"
    )
    to_date: str = Field(
        ..., 
        description="End date in YYYY-MM-DD format",
        example="2024-01-31"
    )
    table_name: str = Field(
        ...,
        description="Database table name for appointment data (required)",
        example="rethinkdump"
    )
    auth_key: Optional[str] = Field(
        None,
        description="Optional API authentication key"
    )

    @field_validator('from_date', 'to_date')
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is YYYY-MM-DD."""
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            raise ValueError('Date must be in YYYY-MM-DD format')

        # Try to parse the date to ensure it's valid
        try:
            datetime.strptime(v, '%Y-%m-%d')
        except ValueError:
            raise ValueError('Invalid date')

        return v

    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v):
        """Validate table name contains only safe characters."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError('Table name must contain only letters, numbers, and underscores, and start with a letter or underscore')
        return v

class DashboardRequest(BaseModel):
    """Request model for Over Term dashboard endpoint."""
    
    start_date: str = Field(
        ...,
        description="Start date in MM/dd/yyyy format (required)",
        example="01/01/2024"
    )
    end_date: str = Field(
        ...,
        description="End date in MM/dd/yyyy format (required)",
        example="01/31/2024"
    )
    client_ids: List[int] = Field(
        ...,
        description="List of client IDs to filter for (required, use empty list [] for all clients)",
        example=[325526, 349284, 304907]
    )
    auth_key: Optional[str] = Field(
        None,
        description="Optional API authentication key"
    )
    
    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is MM/dd/yyyy if provided."""
        if v is None:
            return v

        if not re.match(r'^\d{2}/\d{2}/\d{4}$', v):
            raise ValueError('Date must be in MM/dd/yyyy format')

        # Try to parse the date to ensure it's valid
        try:
            datetime.strptime(v, '%m/%d/%Y')
        except ValueError:
            raise ValueError('Invalid date')

        return v

    @field_validator('client_ids')
    @classmethod
    def validate_client_ids(cls, v):
        """Validate client IDs are positive integers."""
        if v is None:
            return v

        if not isinstance(v, list):
            raise ValueError('client_ids must be a list')

        for client_id in v:
            if not isinstance(client_id, int) or client_id <= 0:
                raise ValueError('All client IDs must be positive integers')

        return v

class OverTermSyncRequest(BaseModel):
    """Request model for Over Term sync endpoint."""
    
    start_date: str = Field(
        ...,
        description="Start date in MM/dd/yyyy format (required)",
        example="01/01/2024"
    )
    end_date: str = Field(
        ...,
        description="End date in MM/dd/yyyy format (required)",
        example="01/31/2024"
    )
    client_ids: List[int] = Field(
        ...,
        description="List of client IDs to filter for (required, use empty list [] for all clients)",
        example=[325526, 349284, 304907]
    )
    table_name: str = Field(
        default="overterm_dashboard",
        description="Database table name for Over Term data",
        example="overterm_dashboard"
    )
    auth_key: Optional[str] = Field(
        None,
        description="Optional API authentication key"
    )
    
    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is MM/dd/yyyy if provided."""
        if v is None:
            return v

        if not re.match(r'^\d{2}/\d{2}/\d{4}$', v):
            raise ValueError('Date must be in MM/dd/yyyy format')

        try:
            datetime.strptime(v, '%m/%d/%Y')
        except ValueError:
            raise ValueError('Invalid date')

        return v

    @field_validator('client_ids')
    @classmethod
    def validate_client_ids(cls, v):
        """Validate client IDs are positive integers."""
        if v is None:
            return v

        if not isinstance(v, list):
            raise ValueError('client_ids must be a list')

        for client_id in v:
            if not isinstance(client_id, int) or client_id <= 0:
                raise ValueError('All client IDs must be positive integers')

        return v

    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v):
        """Validate table name contains only safe characters."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError('Table name must contain only letters, numbers, and underscores, and start with a letter or underscore')
        return v

class SyncResponse(BaseModel):
    """Response model for sync operations."""
    
    status: str = Field(description="Operation status")
    sync_message: Optional[str] = Field(None, description="Status message")
    table_name: Optional[str] = Field(None, description="Target table name")
    records_processed: Optional[int] = Field(None, description="Number of records processed")
    records_inserted: Optional[int] = Field(None, description="Number of records successfully inserted")
    errors: Optional[int] = Field(None, description="Number of errors encountered")
    timestamp: str = Field(description="Operation timestamp")
    duration_seconds: Optional[float] = Field(None, description="Operation duration in seconds")

class DashboardResponse(BaseModel):
    """Response model for dashboard data operations."""
    
    status: str = Field(description="Operation status")
    data: Optional[Dict[str, Any]] = Field(None, description="Dashboard data")
    filters_applied: Optional[Dict[str, Any]] = Field(None, description="Applied filters")
    timestamp: str = Field(description="Operation timestamp")
    duration_seconds: Optional[float] = Field(None, description="Operation duration in seconds")

class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    
    status: str = Field(description="Health status")
    timestamp: str = Field(description="Check timestamp")
    service: str = Field(description="Service name")
    version: str = Field(description="Service version")
    checks: Dict[str, str] = Field(description="Individual check results")

class ErrorResponse(BaseModel):
    """Response model for error responses."""
    
    status: str = Field(default="error", description="Error status")
    message: str = Field(description="Error message")
    timestamp: str = Field(description="Error timestamp")
    detail: Optional[str] = Field(None, description="Additional error details")

class CancelledAppointmentsRequest(BaseModel):
    """Request model for cancelled appointments sync endpoint."""
    
    from_date: str = Field(
        ...,
        description="Start date in YYYY-MM-DD format or UI format (M/D/YYYY, h:mm:ss AM/PM) (required)",
        example="2024-01-01 or 1/1/2024, 12:00:00 AM"
    )
    to_date: str = Field(
        ...,
        description="End date in YYYY-MM-DD format or UI format (M/D/YYYY, h:mm:ss AM/PM) (required)",
        example="2024-01-31 or 1/31/2024, 11:59:59 PM"
    )
    table_name: str = Field(
        ...,
        description="Database table name for cancelled appointments data (required)",
        example="cancelled_appointments"
    )
    truncate: bool = Field(
        default=True,
        description="Whether to truncate the table before inserting data",
        example=True
    )
    auth_key: Optional[str] = Field(
        None,
        description="Optional API authentication key"
    )
    
    @field_validator('from_date', 'to_date')
    @classmethod
    def validate_date_format(cls, v):
        """Validate and convert date format.
        
        Accepts two formats:
        1. YYYY-MM-DD (ISO format)
        2. M/D/YYYY, h:mm:ss AM/PM (UI format)
        
        Returns date in YYYY-MM-DD format.
        """
        # Check for ISO format (YYYY-MM-DD)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            try:
                datetime.strptime(v, '%Y-%m-%d')
                return v  # Already in correct format
            except ValueError:
                raise ValueError('Invalid ISO format date')
        
        # Check for UI format (M/D/YYYY, h:mm:ss AM/PM)
        ui_pattern = r'^\d{1,2}/\d{1,2}/\d{4},\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M$'
        if re.match(ui_pattern, v):
            try:
                # Parse the UI date format
                dt = datetime.strptime(v, '%m/%d/%Y, %I:%M:%S %p')
                # Return in ISO format
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                raise ValueError('Invalid UI format date')
        
        raise ValueError('Date must be in YYYY-MM-DD format or M/D/YYYY, h:mm:ss AM/PM format')


    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v):
        """Validate table name contains only safe characters."""
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', v):
            raise ValueError('Table name must contain only letters, numbers, and underscores, and start with a letter or underscore')
        return v


class ServiceInfo(BaseModel):
    """Response model for service information endpoint."""
    
    service: str = Field(description="Service name")
    version: str = Field(description="Service version")
    status: str = Field(description="Service status")
    endpoints: Dict[str, str] = Field(description="Available endpoints")
