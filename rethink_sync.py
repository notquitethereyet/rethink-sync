#!/usr/bin/env python3
"""
Rethink BH to Supabase appointment sync module.
Handles downloading appointment data and syncing to Supabase database.
"""

import os
import io
import re
import pandas as pd
import psycopg2
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse
from typing import Dict, Any, Optional, List

from config import config, db_config
from logger import get_logger, get_sync_logger, log_performance
from auth import RethinkAuth, RethinkAuthError

# Initialize logging
logger = get_logger(__name__)
sync_logger = get_sync_logger(logger)

class RethinkSyncError(Exception):
    """Custom exception for Rethink sync operations."""
    pass

class RethinkSync:
    """Handles the complete Rethink BH to Supabase appointment sync process."""

    def __init__(self, auth: RethinkAuth = None):
        """
        Initialize RethinkSync.

        Args:
            auth: Optional RethinkAuth instance. If not provided, creates a new one.
        """
        self.auth = auth or RethinkAuth()

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

    def _get_database_url(self) -> str:
        """Get database URL from environment or Secret Manager."""
        db_url = os.getenv("SUPABASE_DB_URL")

        if db_url:
            logger.info("Using database URL from environment variables")
            return db_url

        # If not found, try Secret Manager
        try:
            db_url = self.auth._get_secret("SUPABASE_DB_URL")
            if db_url:
                return db_url
        except RethinkAuthError as e:
            raise RethinkSyncError(f"Failed to get database URL: {str(e)}")

        raise RethinkSyncError("Missing database URL")

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
        Calculate date range based on provided dates with proper validation.

        Returns:
            Tuple of (start_date_str, end_date_str) in MM/DD/YYYY, HH:MM:SS AM/PM format
        """
        # Parse provided dates if any
        start_date = self._parse_date(getattr(self, 'from_date', None))
        end_date = self._parse_date(getattr(self, 'to_date', None))

        # Validate that both dates are provided
        if not hasattr(self, 'from_date') or not self.from_date:
            error_msg = "Missing required parameter: from_date must be provided in YYYY-MM-DD format"
            logger.error(error_msg)
            raise RethinkSyncError(error_msg)

        if not hasattr(self, 'to_date') or not self.to_date:
            error_msg = "Missing required parameter: to_date must be provided in YYYY-MM-DD format"
            logger.error(error_msg)
            raise RethinkSyncError(error_msg)

        # Validate that dates were parsed successfully
        if not start_date:
            error_msg = f"Invalid from_date format: '{self.from_date}'. Expected YYYY-MM-DD format"
            logger.error(error_msg)
            raise RethinkSyncError(error_msg)

        if not end_date:
            error_msg = f"Invalid to_date format: '{self.to_date}'. Expected YYYY-MM-DD format"
            logger.error(error_msg)
            raise RethinkSyncError(error_msg)

        # Ensure times are set correctly for the provided dates
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # Format dates using platform-agnostic string formatting
        start_str = f"{start_date.month}/{start_date.day}/{start_date.year}, {start_date.strftime('%I:%M:%S %p')}"
        end_str = f"{end_date.month}/{end_date.day}/{end_date.year}, {end_date.strftime('%I:%M:%S %p')}"

        logger.info(f"Using date range: {start_str} to {end_str}")
        return start_str, end_str

    def _download_excel(self) -> pd.DataFrame:
        """Download Excel data from Rethink BH API."""
        logger.info("Downloading appointment data")

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
            "sameLocationStaffIds": [],
            "timeFormat": "hh:mm tt",
            "dateFormat": "MM/dd/yyyy",
            "includeAssignedOnly": False,
            "sorting": {"field": "date", "asc": False, "type": 1},
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
                "clientsOptions": []
            },
            "schedulerPermissionLevelTypeId": 2
        }

        try:
            response = self.auth.make_request(
                "POST",
                f"{self.auth.base_url}/core/api/scheduling/scheduling/GetAppointmentsListPrintAsync",
                request_type="scheduler",
                json=payload,
                timeout=120
            )

            # Load Excel data into DataFrame
            excel_data = io.BytesIO(response.content)
            df = pd.read_excel(excel_data, skiprows=1)

            logger.info(f"Downloaded {len(df)} appointment records")
            return df

        except Exception as e:
            logger.error(f"Excel download failed: {e}")
            raise RethinkSyncError(f"Excel download failed: {e}")

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
                # First check if the table exists (case-insensitive)
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND lower(table_name) = lower(%s)
                    )
                """, (self.table_name,))
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logger.error(f"Table '{self.table_name}' does not exist in the database")
                    raise RethinkSyncError(f"Table '{self.table_name}' does not exist in the database. Please create the table first or check the table name.")

                # Truncate the table
                cur.execute(f'TRUNCATE TABLE "{self.table_name}" CASCADE')
                logger.info(f"Table '{self.table_name}' truncated")

                # Try resetting the serial sequence (only if one exists)
                cur.execute(f"""
                    SELECT pg_get_serial_sequence('"{self.table_name}"', 'id')
                """)
                result = cur.fetchone()
                seq_name = result[0] if result else None

                if seq_name:
                    cur.execute(f"SELECT setval('{seq_name}', 1, false)")
                    logger.info(f"Reset sequence {seq_name} for table '{self.table_name}'")
                else:
                    logger.warning(f"No serial sequence found for 'id' in table '{self.table_name}'")

                conn.commit()
        except psycopg2.errors.UndefinedTable as e:
            logger.error(f"Table '{self.table_name}' does not exist")
            raise RethinkSyncError(f"Table '{self.table_name}' does not exist in the database. Please create the table first or check the table name.")
        except Exception as e:
            logger.error(f"Table truncation failed: {str(e)}")
            raise RethinkSyncError(f"Table truncation failed: {str(e)}")

    def _map_excel_to_db_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Map Excel columns to database columns."""
        mapped_columns = {}
        for excel_col in df.columns:
            if excel_col in self.column_mapping:
                db_col = self.column_mapping[excel_col]
                mapped_columns[db_col] = excel_col
        return mapped_columns

    def _generate_name_code(self, full_name: str) -> str:
        """
        Generate a 4-character name code from a full name for privacy/compression.

        Format: FirstNameInitials + LastNameInitials (mixed case)
        - Takes first 2 letters of first name + first 2 letters of last name
        - Capitalizes first letter of each pair, lowercase for second letter
        - Removes special characters (apostrophes, hyphens, etc.)
        - Handles nicknames in parentheses by ignoring them

        Examples:
        - "Doe, John (Jane)" -> "JoDo"
        - "O'Connor, Patrick" -> "PaOc"
        - "Smith-Jones, Mary-Ann" -> "MaSm"
        - "John, Doe" -> "JoDo"

        Args:
            full_name: Full name in "Last, First" or "Last, First (Nickname)" format

        Returns:
            Generated name code (4 characters in mixed case)
        """
        if not full_name or not isinstance(full_name, str):
            return "UnKn"  # Default for unknown names

        try:
            # Clean the name and split by comma
            cleaned_name = full_name.strip()

            # Handle names with parentheses (nicknames)
            # Remove anything in parentheses first
            cleaned_name = re.sub(r'\s*\([^)]*\)', '', cleaned_name)

            # Split by comma to get last and first names
            if ',' in cleaned_name:
                parts = cleaned_name.split(',', 1)
                last_name = parts[0].strip()
                first_name = parts[1].strip()
            else:
                # If no comma, assume it's "First Last" format
                name_parts = cleaned_name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                else:
                    # Single name, use first 4 characters with proper case
                    single_code = (cleaned_name[:4] + "XXXX")[:4]
                    return single_code.capitalize() + single_code[2:].capitalize()[:2]

            # Clean names by removing special characters and keeping only letters
            first_name_clean = re.sub(r'[^a-zA-Z]', '', first_name)
            last_name_clean = re.sub(r'[^a-zA-Z]', '', last_name)

            # Extract first 2 characters from cleaned first name and capitalize properly
            first_code = first_name_clean[:2] if len(first_name_clean) >= 2 else (first_name_clean + "X")[:2]
            first_code = first_code.capitalize()  # First letter uppercase, second lowercase

            # Extract first 2 characters from cleaned last name and capitalize properly
            last_code = last_name_clean[:2] if len(last_name_clean) >= 2 else (last_name_clean + "X")[:2]
            last_code = last_code.capitalize()  # First letter uppercase, second lowercase

            # Combine first name code + last name code
            name_code = first_code + last_code

            logger.debug(f"Generated name code '{name_code}' for '{full_name}'")
            return name_code

        except Exception as e:
            logger.warning(f"Error generating name code for '{full_name}': {e}")
            return "UnKn"

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
                
                # Convert client name to nameCode for privacy
                if db_col == 'client' and not pd.isna(value):
                    value = self._generate_name_code(str(value))
                
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

    @log_performance
    def run_sync(self, from_date: str, to_date: str, table_name: str) -> Dict[str, Any]:
        """Execute the complete sync process.
        
        Args:
            from_date: Start date in YYYY-MM-DD format (required)
            to_date: End date in YYYY-MM-DD format (required)
            table_name: Name of the table to insert data into (required)
        
        Returns:
            Dictionary containing sync results and statistics
        
        Raises:
            RethinkSyncError: If any required parameters are missing or invalid
        """
        # Validate required parameters
        if from_date is None:
            logger.error("Missing required parameter: from_date")
            raise RethinkSyncError("Missing required parameter: from_date")
            
        if to_date is None:
            logger.error("Missing required parameter: to_date")
            raise RethinkSyncError("Missing required parameter: to_date")
            
        if table_name is None or table_name.strip() == '':
            logger.error("Missing required parameter: table_name")
            raise RethinkSyncError("Missing required parameter: table_name")
        
        self.from_date = from_date
        self.to_date = to_date
        self.table_name = table_name

        # Start sync logging
        sync_id = sync_logger.log_sync_start("appointment_sync", {
            "from_date": from_date,
            "to_date": to_date,
            "table_name": table_name
        })

        try:
            # Get database URL
            db_url = self._get_database_url()

            # Authenticate with Rethink BH (auth module handles credentials)
            if not self.auth.is_authenticated:
                self.auth.authenticate()

            # Download Excel data
            df = self._download_excel()

            # Connect to database
            conn = self._connect_database(db_url)

            try:
                # Truncate table
                self._truncate_table(conn)

                # Insert data
                _, error_count = self._insert_data(conn, df)

                logger.info(f"Sync completed: {len(df)} records, {error_count} errors")

                # Log sync completion
                sync_logger.log_sync_complete(sync_id, {
                    "rows_processed": len(df),
                    "rows_inserted": len(df) - error_count,
                    "errors": error_count
                })

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
            sync_logger.log_sync_error(sync_id, "RethinkSyncError occurred")
            raise
        except Exception as e:
            sync_logger.log_sync_error(sync_id, str(e))
            logger.error(f"Unexpected error during sync: {str(e)}")
            raise RethinkSyncError(f"Sync failed: {str(e)}")
