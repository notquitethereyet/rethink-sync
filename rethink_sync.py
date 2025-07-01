#!/usr/bin/env python3
"""
Unified Rethink BH to Supabase sync module.
Combines download and ingestion logic for Cloud Run deployment.
"""

import os
import io
import logging
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
from typing import Dict, Any, Optional
from google.cloud import secretmanager

# Configure logging
logger = logging.getLogger(__name__)

class RethinkSyncError(Exception):
    """Custom exception for Rethink sync operations."""
    pass

class RethinkSync:
    """Handles the complete Rethink BH to Supabase sync process."""
    
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
        
        # Column mapping for Excel to database
        self.column_mapping = {
            'Appointment Type': 'appointmentType',
            'Appointment Tag': 'appointmentTag', 
            'Service Line': 'serviceLine',
            'Service': 'service',
            'Appointment Location': 'appointmentLocation',
            'Duration': 'duration',
            'Day': 'day',
            'Date': 'date',
            'Time': 'time',
            'Scheduled Date': 'scheduledDate',
            'Modified Date': 'modifiedDate',
            'Client': 'client',
            'Staff Member': 'staff',
            'Status': 'status',
            'Session Note': 'sessionNote',
            'Staff Verification': 'staffVerification',
            'Staff Verification Address': 'staffVerificationAddress',
            'Guardian Verification': 'guardianVerification',
            'Parent Verification Address': 'parentVerificationAddress',
            'PayCode Name': 'paycodeName',
            'PayCode': 'paycode',
            'Notes': 'notes',
            'Appointment ID': 'appointmentID',
            'Validation': 'validation',
            'Place of Service': 'placeOfService'
        }

    def _get_secret(self, secret_name: str, project_id: Optional[str] = None) -> str:
        """Retrieve secret from Google Cloud Secret Manager."""
        try:
            if project_id is None:
                project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
                if not project_id:
                    raise RethinkSyncError("GOOGLE_CLOUD_PROJECT environment variable not set")
            
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}")
            raise RethinkSyncError(f"Secret retrieval failed: {str(e)}")

    def _get_credentials(self) -> tuple[str, str, str]:
        """Get credentials from environment or Secret Manager."""
        # Try environment variables first (for local development)
        email = os.getenv("RTHINK_USER")
        password = os.getenv("RTHINK_PASS")
        db_url = os.getenv("SUPABASE_DB_URL")

        # If all environment variables are found, use them
        if all([email, password, db_url]):
            logger.info("Using credentials from environment variables")
            return email, password, db_url

        # If not found, try Secret Manager
        try:
            if not email:
                email = self._get_secret("RTHINK_USER")
            if not password:
                password = self._get_secret("RTHINK_PASS")
            if not db_url:
                db_url = self._get_secret("SUPABASE_DB_URL")
        except RethinkSyncError as e:
            # If Secret Manager fails and we don't have env vars, raise error
            if not all([email, password, db_url]):
                raise RethinkSyncError("Missing required credentials and Secret Manager unavailable")

        if not all([email, password, db_url]):
            raise RethinkSyncError("Missing required credentials")

        return email, password, db_url

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
            raise RethinkSyncError("No anti-forgery token found in cookies")
        return {**headers, "X-XSRF-TOKEN": token}

    def _authenticate(self, email: str, password: str) -> None:
        """Authenticate with Rethink BH."""
        logger.info("Starting authentication with Rethink BH")
        
        try:
            # Initial request to get session
            self.session.get(f"{self.base_url}/HealthCare", headers=self.headers).raise_for_status()
            
            # Verify email
            self.session.post(
                f"{self.base_url}/HealthCare/SingleSignOn/GetAuthenticationDetail",
                json={"User": email}, 
                headers=self._with_token(self.headers)
            ).raise_for_status()
            
            # Login
            self.session.post(
                f"{self.base_url}/HealthCare/User/Login",
                json={"User": email, "Password": password, "setPermissions": True},
                headers=self._with_token(self.headers)
            ).raise_for_status()
            
            # Final authentication step
            self.session.get(
                f"{self.base_url}/core/scheduler/appointments",
                headers=self._with_token(self.headers)
            ).raise_for_status()
            
            logger.info("Authentication successful")
            
        except requests.RequestException as e:
            logger.error("Authentication failed")
            raise RethinkSyncError(f"Authentication failed: {str(e)}")

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse a date string in YYYY-MM-DD format to datetime."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError as e:
            logger.warning(f"Invalid date format: {date_str}, expected YYYY-MM-DD. Error: {e}")
            return None
            
    def _get_date_range(self) -> tuple[str, str]:
        """
        Calculate date range based on provided dates or default to current month.
        
        Returns:
            Tuple of (start_date_str, end_date_str) in MM/DD/YYYY, HH:MM:SS AM/PM format
        """
        # Parse provided dates if any
        start_date = self._parse_date(getattr(self, 'from_date', None))
        end_date = self._parse_date(getattr(self, 'to_date', None))
        
        # If only one date is provided, use it for both start and end
        if start_date and not end_date:
            end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=0)
        elif end_date and not start_date:
            start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # If no dates provided or parsing failed, use current month
        if not start_date or not end_date:
            # Get start of current month at 12:00:00 AM
            start_date = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Get end of current month at 11:59:59 PM
            end_date = start_date + relativedelta(months=1, days=-1, hours=23, minutes=59, seconds=59)
        else:
            # Ensure times are set correctly for the provided dates
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)

        # Format dates using platform-agnostic string formatting
        start_str = f"{start_date.month}/{start_date.day}/{start_date.year}, {start_date.strftime('%I:%M:%S %p')}"
        end_str = f"{end_date.month}/{end_date.day}/{end_date.year}, {end_date.strftime('%I:%M:%S %p')}"

        logger.info(f"Using date range: {start_str} to {end_str}")
        return start_str, end_str
    def _download_excel(self) -> pd.DataFrame:
        """Download Excel data from Rethink BH API and return as DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame containing the Excel data
        """
        logger.info("Downloading Excel data from Rethink BH")
        
        start_date, end_date = self._get_date_range()
        
        payload = {
            "startDate": start_date,
            "endDate": end_date,
            "checkExceedAuthorization": True,
            "showEVV": 0,
            "memberIds": [],
            "clientIds": [],
            "staffIds": [],
            "skip": 100,
            "pageSize": 100,
            "sameLocationStaffIds": [],  # Required field from original
            "timeFormat": "hh:mm tt",
            "dateFormat": "MM/dd/yyyy",
            "includeAssignedOnly": False,
            "sorting": {
                "field": "date",
                "asc": False,
                "type": 1
            },
            "filterOptions": {
                "startDate": start_date,
                "endDate": end_date,
                "appointmentStatus": [
                    "Scheduled", "Needs Verification", "Completed",
                    "Cancelled - Needs Reschedule", "Cancelled - Rescheduled",
                    "Cancelled - Cannot Reschedule"
                ],
                "billedStatus": [],
                "sessionNoteStatus": [],
                "sessionNoteAlerts": [],
                "appointmentLocation": [],
                "location": [],
                "appointmentType": [],
                "payableTypes": [],
                "authRequirements": [],
                "CMFDate": None,
                "CMTDate": None,
                "appointmentRequirements": ["staffVerification"],
                "authorizationType": [],
                "appointmentId": None,
                "authorizationNumber": "",
                "billingCodes": None,
                "acknowledgeableExceptions": [],
                "EVVReasonCodes": [],
                "EVVStatus": [],
                "missingClockedIn": None,
                "missingClockedOut": None,
                "renderingProviders": [],
                "service": [],
                "serviceLineId": None,
                "showEVV": False,
                "memberIds": [],
                "clientIds": [],
                "staffIds": [],
                "validations": [],
                "clientsOptions": [],
                # "includeAssignedOnly": False
            },
            "schedulerPermissionLevelTypeId": 2
        }
        
        try:
            # Refresh session by visiting scheduler page again
            logger.info("Refreshing session before Excel download")
            self.session.get(
                f"{self.base_url}/core/scheduler/appointments",
                headers=self._with_token(self.headers)
            ).raise_for_status()

            # Get a fresh token right before the request
            fresh_headers = self._with_token(self.headers)
            logger.info("Making Excel download request with fresh token")

            response = self.session.post(
                f"{self.base_url}/core/api/scheduling/scheduling/GetAppointmentsListPrintAsync",
                json=payload,
                headers=fresh_headers,
                timeout=120,
            )

            if response.status_code != 200:
                logger.error(f"Excel download failed with status {response.status_code}")
                logger.error(f"Response content: {response.text[:500]}")
                response.raise_for_status()

            # Load Excel data into DataFrame
            excel_data = io.BytesIO(response.content)
            df = pd.read_excel(excel_data, skiprows=1)  # Skip empty first row

            logger.info(f"Downloaded {len(df)} rows from Rethink BH")
            return df

        except Exception as e:
            logger.error("Excel download failed")
            raise RethinkSyncError(f"Excel download failed: {str(e)}")

    def _connect_database(self, db_url: str):
        """Connect to Supabase PostgreSQL database."""
        try:
            parsed = urlparse(db_url)
            conn = psycopg2.connect(
                dbname=parsed.path[1:],  # Remove leading slash
                user=parsed.username,
                password=parsed.password,
                host=parsed.hostname,
                port=parsed.port
            )
            logger.info("Connected to Supabase database")
            return conn
        except Exception as e:
            logger.error("Database connection failed")
            raise RethinkSyncError(f"Database connection failed: {str(e)}")

    def _truncate_table(self, conn) -> None:
        """Truncate the specified table and reset ID sequence."""
        try:
            with conn.cursor() as cur:
                # Truncate the table and reset the sequence
                cur.execute(f'TRUNCATE TABLE "{self.table_name}" RESTART IDENTITY CASCADE')
                conn.commit()
                logger.info(f"Table '{self.table_name}' truncated and ID sequence reset")
        except Exception as e:
            logger.error("Table truncation failed")
            raise RethinkSyncError(f"Table truncation failed: {str(e)}")

    def _map_excel_to_db_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Map Excel columns to database columns."""
        mapped_columns = {}
        for excel_col in df.columns:
            if excel_col in self.column_mapping:
                db_col = self.column_mapping[excel_col]
                mapped_columns[db_col] = excel_col
        return mapped_columns

    def _prepare_row_data(self, row: pd.Series, column_mapping: Dict[str, str]) -> list:
        """Prepare a single row for database insertion."""
        db_columns = [
            'appointmentType', 'appointmentTag', 'serviceLine', 'service',
            'appointmentLocation', 'duration', 'day', 'date', 'time',
            'scheduledDate', 'modifiedDate', 'client', 'staff', 'status',
            'sessionNote', 'staffVerification', 'staffVerificationAddress',
            'guardianVerification', 'parentVerificationAddress',
            'paycodeName', 'paycode', 'notes', 'appointmentID',
            'validation', 'placeOfService'
        ]

        values = []
        for db_col in db_columns:
            if db_col in column_mapping:
                excel_col = column_mapping[db_col]
                value = row[excel_col]
                if pd.isna(value):
                    values.append(None)
                else:
                    values.append(value)
            else:
                values.append(None)

        return values

    def _insert_data(self, conn, df: pd.DataFrame) -> tuple[int, int]:
        """Insert DataFrame data into database using batch operations."""
        logger.info("Starting data insertion")

        column_mapping = self._map_excel_to_db_columns(df)
        success_count = 0
        error_count = 0

        # Prepare all rows for batch insertion
        all_values = []
        for index, row in df.iterrows():
            try:
                values = self._prepare_row_data(row, column_mapping)
                all_values.append(values)
            except Exception as e:
                error_count += 1
                logger.warning(f"Error preparing row {index + 1}: {str(e)}")

        # Batch size for insertion
        batch_size = 100

        try:
            with conn.cursor() as cur:
                for i in range(0, len(all_values), batch_size):
                    batch = all_values[i:i+batch_size]

                    # Construct batch insert query with lowercase column names
                    args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8') for x in batch)
                    query = f"""
                        INSERT INTO "{self.table_name}" (
                            "appointmenttype", "appointmenttag", "serviceline", "service",
                            "appointmentlocation", "duration", "day", "date", "time",
                            "scheduleddate", "modifieddate", "client", "staff", "status",
                            "sessionnote", "staffverification", "staffverificationaddress",
                            "guardianverification", "parentverificationaddress",
                            "paycodename", "paycode", "notes", "appointmentid",
                            "validation", "placeofservice"
                        ) VALUES """ + args_str
                    cur.execute(query)

                    success_count += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1}, rows {i+1}-{i+len(batch)}")

                conn.commit()
                logger.info(f"Data insertion completed: {success_count} success, {error_count} errors")

        except Exception as e:
            conn.rollback()
            logger.error("Data insertion failed")
            raise RethinkSyncError(f"Data insertion failed: {str(e)}")

        return success_count, error_count

    def run_sync(self, from_date: Optional[str] = None, to_date: Optional[str] = None, table_name: str = 'rethinkdump') -> Dict[str, Any]:
        """Execute the complete sync process.
        
        Args:
            from_date: Optional start date in YYYY-MM-DD format
            to_date: Optional end date in YYYY-MM-DD format
            table_name: Name of the table to insert data into (default: 'rethinkDump')
            
        Returns:
            Dictionary containing sync results and statistics
        """
        self.from_date = from_date
        self.to_date = to_date
        self.table_name = table_name
        
        try:
            # Get credentials
            email, password, db_url = self._get_credentials()

            # Authenticate with Rethink BH
            self._authenticate(email, password)

            # Download Excel data
            df = self._download_excel()

            # Connect to database
            conn = self._connect_database(db_url)

            try:
                # Truncate table
                self._truncate_table(conn)

                # Insert data
                success_count, error_count = self._insert_data(conn, df)

                logger.info("Sync completed successfully")
                return {
                    "status": "success",
                    "table": self.table_name,
                    "rows_processed": len(df),
                    "rows_inserted": len(df) - error_count,
                    "errors": error_count,
                    "total_rows": len(df)
                }

            finally:
                conn.close()

        except RethinkSyncError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during sync: {str(e)}")
            raise RethinkSyncError(f"Sync failed: {str(e)}")
