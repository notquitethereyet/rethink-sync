#!/usr/bin/env python3
"""
Centralized logging module for Rethink BH Sync application.
Provides structured logging with consistent formatting and context.
"""

import logging
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps
from config import config

class StructuredLogger:
    """Enhanced logger with structured logging capabilities."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._setup_logger()
        
    def _setup_logger(self):
        """Setup logger with consistent configuration."""
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Set level
        self.logger.setLevel(getattr(logging, config.LOG_LEVEL))
        
        # Create formatter
        formatter = logging.Formatter(config.LOG_FORMAT)
        
        # Create and configure handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(handler)
        
        # Prevent propagation to avoid duplicate logs
        self.logger.propagate = False
    
    def info(self, message: str, **kwargs):
        """Log info message with optional context."""
        self._log_with_context(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message with optional context."""
        self._log_with_context(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message with optional context."""
        self._log_with_context(logging.ERROR, message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message with optional context."""
        self._log_with_context(logging.DEBUG, message, **kwargs)
    
    def _log_with_context(self, level: int, message: str, **kwargs):
        """Log message with additional context."""
        if kwargs:
            context_str = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            message = f"{message} | {context_str}"
        
        self.logger.log(level, message)

class RequestLogger:
    """Logger for HTTP request lifecycle tracking."""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
    
    def log_request_start(self, method: str, path: str, request_id: str = None) -> str:
        """Log the start of a request and return request ID."""
        if not request_id:
            request_id = str(uuid.uuid4())[:8]
        
        self.logger.info(
            "REQUEST_START",
            method=method,
            path=path,
            request_id=request_id,
            timestamp=datetime.now().isoformat()
        )
        return request_id
    
    def log_request_complete(self, request_id: str, status_code: int, duration: float):
        """Log the completion of a request."""
        self.logger.info(
            "REQUEST_COMPLETE",
            request_id=request_id,
            status_code=status_code,
            duration_ms=round(duration * 1000, 2),
            timestamp=datetime.now().isoformat()
        )
    
    def log_request_error(self, request_id: str, error: str, status_code: int = 500):
        """Log a request error."""
        self.logger.error(
            "REQUEST_ERROR",
            request_id=request_id,
            error=error,
            status_code=status_code,
            timestamp=datetime.now().isoformat()
        )

class SyncLogger:
    """Logger for sync operation tracking."""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
    
    def log_sync_start(self, sync_type: str, params: Dict[str, Any] = None) -> str:
        """Log the start of a sync operation."""
        sync_id = str(uuid.uuid4())[:8]
        
        log_data = {
            "sync_id": sync_id,
            "sync_type": sync_type,
            "timestamp": datetime.now().isoformat()
        }
        
        if params:
            log_data.update(params)
        
        self.logger.info("SYNC_START", **log_data)
        return sync_id
    
    def log_sync_complete(self, sync_id: str, stats: Dict[str, Any]):
        """Log the completion of a sync operation."""
        self.logger.info(
            "SYNC_COMPLETE",
            sync_id=sync_id,
            timestamp=datetime.now().isoformat(),
            **stats
        )
    
    def log_sync_error(self, sync_id: str, error: str):
        """Log a sync operation error."""
        self.logger.error(
            "SYNC_ERROR",
            sync_id=sync_id,
            error=error,
            timestamp=datetime.now().isoformat()
        )

class AuthLogger:
    """Logger for authentication events."""
    
    def __init__(self, logger: StructuredLogger):
        self.logger = logger
    
    def log_auth_success(self, user: str = None):
        """Log successful authentication."""
        self.logger.info(
            "AUTH_SUCCESS",
            user=user or "system",
            timestamp=datetime.now().isoformat()
        )
    
    def log_auth_failure(self, reason: str, user: str = None):
        """Log authentication failure."""
        self.logger.warning(
            "AUTH_FAILURE",
            user=user or "unknown",
            reason=reason,
            timestamp=datetime.now().isoformat()
        )
    
    def log_rate_limit_exceeded(self, ip: str = None):
        """Log rate limit exceeded event."""
        self.logger.warning(
            "RATE_LIMIT_EXCEEDED",
            ip=ip or "unknown",
            timestamp=datetime.now().isoformat()
        )

def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name)

def get_request_logger(logger: StructuredLogger) -> RequestLogger:
    """Get a request logger instance."""
    return RequestLogger(logger)

def get_sync_logger(logger: StructuredLogger) -> SyncLogger:
    """Get a sync logger instance."""
    return SyncLogger(logger)

def get_auth_logger(logger: StructuredLogger) -> AuthLogger:
    """Get an auth logger instance."""
    return AuthLogger(logger)

def log_performance(func):
    """Decorator to log function performance."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            if duration > 5.0:  # Log slow operations
                logger.warning(
                    "SLOW_OPERATION",
                    function=func.__name__,
                    duration_seconds=round(duration, 2)
                )
            
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                "OPERATION_ERROR",
                function=func.__name__,
                error=str(e),
                duration_seconds=round(duration, 2)
            )
            raise
    
    return wrapper
