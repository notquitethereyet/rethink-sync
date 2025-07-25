#!/usr/bin/env python3
"""
Authentication module for Rethink BH API.
Handles login, session management, and token generation.
"""

import os
import logging
import requests
import re
from typing import Optional, Dict, Tuple
from google.cloud import secretmanager

# Configure structured logging
logger = logging.getLogger(__name__)

class RethinkAuthError(Exception):
    """Custom exception for Rethink authentication operations."""
    pass

class RethinkAuth:
    """Handles authentication and session management for Rethink BH API."""
    
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://webapp.rethinkbehavioralhealth.com"
        self.headers = {
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json, text/plain, */*",
            "X-Application-Key": "74569e11-18b4-4122-a58d-a4b830aa12c4",
            "X-Origin": "Angular",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:139.0) Gecko/139.0",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/Healthcare#/Login",
        }
        self._authenticated = False

    def _get_secret(self, secret_name: str, project_id: Optional[str] = None) -> str:
        """Retrieve secret from Google Cloud Secret Manager."""
        try:
            if project_id is None:
                project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
                if not project_id:
                    raise RethinkAuthError("GOOGLE_CLOUD_PROJECT environment variable not set")
            
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}")
            raise RethinkAuthError(f"Secret retrieval failed: {str(e)}")

    def get_credentials(self) -> Tuple[str, str]:
        """Get credentials from environment or Secret Manager."""
        email = os.getenv("RTHINK_USER")
        password = os.getenv("RTHINK_PASS")

        if email and password:
            logger.debug("Using environment credentials")
            return email, password

        # Fallback to Secret Manager
        try:
            email = email or self._get_secret("RTHINK_USER")
            password = password or self._get_secret("RTHINK_PASS")
            logger.debug("Using Secret Manager credentials")
            return email, password
        except Exception as e:
            raise RethinkAuthError(f"Failed to get credentials: {e}")

    def _fetch_token(self) -> Optional[str]:
        """Extract anti-forgery token from session cookies."""
        for cookie in self.session.cookies:
            if any(k in cookie.name.upper() for k in ("XSRF", "ANTIFORGERY", "REQUESTVERIFICATIONTOKEN")):
                return cookie.value
        return None

    def _with_token(self, headers: dict) -> dict:
        """Add anti-forgery token to headers."""
        token = self._fetch_token()
        if not token:
            raise RethinkAuthError("No anti-forgery token found in cookies")
        return {**headers, "X-XSRF-TOKEN": token}

    def authenticate(self, email: str = None, password: str = None) -> None:
        """Authenticate with Rethink BH."""
        if self._authenticated:
            logger.debug("Already authenticated")
            return

        if not email or not password:
            email, password = self.get_credentials()

        logger.info("Authenticating with Rethink BH")

        try:
            # Authentication flow
            self.session.get(f"{self.base_url}/HealthCare", headers=self.headers).raise_for_status()

            self.session.post(
                f"{self.base_url}/HealthCare/SingleSignOn/GetAuthenticationDetail",
                json={"User": email},
                headers=self._with_token(self.headers)
            ).raise_for_status()

            self.session.post(
                f"{self.base_url}/HealthCare/User/Login",
                json={"User": email, "Password": password, "setPermissions": True},
                headers=self._with_token(self.headers)
            ).raise_for_status()

            self.session.get(
                f"{self.base_url}/core/scheduler/appointments",
                headers=self._with_token(self.headers)
            ).raise_for_status()

            self._authenticated = True
            logger.info("Authentication successful")

        except requests.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            raise RethinkAuthError(f"Authentication failed: {e}")

    def get_api_headers(self, request_type: str = "dashboard") -> Dict[str, str]:
        """Get headers with fresh tokens for API requests."""
        if not self._authenticated:
            raise RethinkAuthError("Must authenticate first")

        # Get XSRF token from session
        xsrf_headers = self._with_token(self.headers)
        xsrf_token = xsrf_headers.get("X-XSRF-TOKEN", "")

        if request_type == "dashboard":
            # Dashboard requests need MVC token
            mvc_token = self._get_mvc_token(xsrf_headers)
            return {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=utf-8",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/Healthcare",
                "User-Agent": self.headers["User-Agent"],
                "X-Application-Key": self.headers["X-Application-Key"],
                "X-Origin": self.headers["X-Origin"],
                "X-XSRF-TOKEN": xsrf_token,
                "X-XSRF-MVC-TOKEN": mvc_token
            }
        else:
            # Appointment/scheduler requests use simpler headers
            return {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=utf-8",
                "User-Agent": self.headers["User-Agent"],
                "X-Application-Key": self.headers["X-Application-Key"],
                "X-Origin": self.headers["X-Origin"],
                "X-XSRF-TOKEN": xsrf_token
            }

    def _get_mvc_token(self, headers: Dict[str, str]) -> str:
        """Extract MVC token from dashboard page."""
        try:
            response = self.session.get(f"{self.base_url}/Healthcare/ReportingDashboard", headers=headers)
            if response.status_code != 200:
                logger.warning(f"Failed to get dashboard page: {response.status_code}")
                return headers.get("X-XSRF-TOKEN", "")

            # Extract token from HTML
            patterns = [
                r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                r'"__RequestVerificationToken":"([^"]+)"'
            ]

            for pattern in patterns:
                match = re.search(pattern, response.text)
                if match:
                    token = match.group(1)
                    logger.debug("Retrieved MVC token")
                    return token

            logger.warning("MVC token not found, using XSRF token")
            return headers.get("X-XSRF-TOKEN", "")

        except Exception as e:
            logger.warning(f"Error getting MVC token: {e}")
            return headers.get("X-XSRF-TOKEN", "")

    def make_request(self, method: str, url: str, request_type: str = "dashboard", **kwargs) -> requests.Response:
        """Make authenticated request to Rethink BH API."""
        if not self._authenticated:
            self.authenticate()

        # Visit appropriate pages based on request type
        if request_type == "scheduler":
            self._visit_scheduler_pages()
        elif request_type == "dashboard":
            self._visit_dashboard_pages()

        headers = self.get_api_headers(request_type)
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
        kwargs['headers'] = headers

        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"API request failed: {method} {url} - {e}")
            raise RethinkAuthError(f"API request failed: {e}")

    def _visit_scheduler_pages(self) -> None:
        """Visit scheduler pages to establish session context."""
        try:
            logger.debug("Visiting scheduler pages")
            self.session.get(
                f"{self.base_url}/core/scheduler/appointments",
                headers=self._with_token(self.headers)
            ).raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to visit scheduler pages: {e}")

    def _visit_dashboard_pages(self) -> None:
        """Visit dashboard pages to establish session context."""
        try:
            logger.debug("Visiting dashboard pages")
            self.session.get(
                f"{self.base_url}/Healthcare",
                headers=self._with_token(self.headers)
            ).raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to visit dashboard pages: {e}")

    @property
    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        return self._authenticated
