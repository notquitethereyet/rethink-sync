import logging
import json
import os
import time
import uuid
import datetime
import psycopg2
from typing import Dict, List, Optional, Any, Tuple
from dateutil.parser import parse as parse_date
from urllib.parse import urlparse

from auth import RethinkAuth, RethinkAuthError
from logger import get_logger, get_sync_logger, log_performance

# Initialize logging
logger = get_logger(__name__)
sync_logger = get_sync_logger(logger)

class CancelledAppointmentsError(Exception):
    """Custom exception for cancelled appointments operations."""
    pass

class CancelledAppointmentsFetcher:
    """
    Class to fetch cancelled appointments from Rethink BH using the paginated API.
    This uses the same authentication flow as the main rethink_sync module but
    specifically targets cancelled appointments using the web UI's paginated endpoint.
    Also handles inserting the data into a specified database table.
    """
    
    def __init__(self, auth: Optional[RethinkAuth], table_name: str):
        """
        Initialize the CancelledAppointmentsFetcher with auth instance and table name.
        
        Args:
            auth: RethinkAuth instance. Required for API authentication.
            table_name: Name of the database table to insert data into. Required parameter.
            
        Raises:
            CancelledAppointmentsError: If table_name is not provided or invalid.
        """
        if not auth:
            logger.info("No auth instance provided, creating a new one")
            auth = RethinkAuth()
            
        if not table_name:
            logger.error("Missing required parameter: table_name must be provided")
            raise CancelledAppointmentsError("Missing required parameter: table_name must be provided")
            
        if not isinstance(table_name, str):
            logger.error(f"Invalid table_name parameter: expected string, got {type(table_name).__name__}")
            raise CancelledAppointmentsError(f"Invalid table_name parameter: expected string, got {type(table_name).__name__}")
            
        self.auth = auth
        self.page_size = 100  # Default page size as observed in the web UI
        self.table_name = table_name
        logger.debug(f"Initialized CancelledAppointmentsFetcher with table_name='{table_name}'")

    
    def _prepare_request_payload(self, from_date: datetime.datetime, 
                               to_date: datetime.datetime, 
                               skip: int = 0) -> Dict[str, Any]:
        """
        Prepare the request payload for the GetEventsListNewAsync endpoint.
        
        Args:
            from_date: Start date for appointment search
            to_date: End date for appointment search
            skip: Number of records to skip (for pagination)
            
        Returns:
            Dict containing the request payload
        """
        # Format dates as expected by the API - using the format from the sample browser request
        from_date_str = from_date.strftime("%m/%d/%Y, %I:%M:%S %p")
        to_date_str = to_date.strftime("%m/%d/%Y, %I:%M:%S %p")
        
        # Build payload based on the sample browser request structure
        payload = {
            "startDate": from_date_str,
            "endDate": to_date_str,
            "checkExceedAuthorization": True,
            "showEVV": 0,
            "memberIds": [],  # We don't have specific member IDs, so leave empty
            "clientIds": [],
            "staffIds": [],
            "timeFormat": "hh:mm tt",
            "dateFormat": "MM/dd/yyyy",
            "skip": skip,
            "pageSize": self.page_size,
            "includeAssignedOnly": False,
            "sorting": {
                "field": "date",
                "asc": False,
                "type": 1
            },
            "filterOptions": {
                "startDate": from_date_str,
                "endDate": to_date_str,
                "appointmentStatus": [
                    "Cancelled - Needs Reschedule",
                    "Cancelled - Rescheduled",
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
                "clientIds": [],
                "staffIds": [],
                "validations": [],
                "clientsOptions": []
            },
            "schedulerPermissionLevelTypeId": 2,
            "funderIds": []
        }
        
        return payload
    
    def fetch_cancelled_appointments(self, from_date: datetime.datetime, 
                                    to_date: datetime.datetime) -> List[Dict[str, Any]]:
        """
        Fetch all cancelled appointments within the given date range.
        
        Args:
            from_date: Start date for appointment search
            to_date: End date for appointment search
            
        Returns:
            List of cancelled appointment objects
        """
        logger.info(f"Fetching cancelled appointments from {from_date} to {to_date}")
        
        # Ensure auth is ready
        if not self.auth.is_authenticated:
            self.auth.authenticate()
        
        all_appointments = []
        skip = 0
        has_more = True
        
        # Handle pagination by fetching until no more results
        while has_more:
            logger.debug(f"Fetching page with skip={skip}, page_size={self.page_size}")
            
            payload = self._prepare_request_payload(from_date, to_date, skip)
            
            # Make the request using the scheduler request type
            api_path = "/core/api/scheduling/scheduling/GetEventsListNewAsync"
            full_url = f"{self.auth.base_url}{api_path}"
            
            response = self.auth.make_request(
                "POST",
                full_url,
                json=payload,
                request_type="scheduler"
            )
            
            # Check if request was successful
            if response.status_code != 200:
                logger.error(f"Failed to fetch cancelled appointments: {response.status_code} {response.text}")
                raise Exception(f"Failed to fetch cancelled appointments: {response.status_code}")
            
            # Parse response
            data = response.json()
            
            # Extract appointments from the response
            appointments = data.get("events", [])
            logger.info(f"Fetched {len(appointments)} cancelled appointments")
            
            # Add to our collection
            all_appointments.extend(appointments)
            
            # Check if we need to fetch more pages
            if len(appointments) < self.page_size:
                has_more = False
            else:
                skip += self.page_size
        
        logger.info(f"Total cancelled appointments fetched: {len(all_appointments)}")
        return all_appointments
    
    def process_appointments(self, appointments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process the raw appointment data to extract relevant fields.
        
        Args:
            appointments: List of raw appointment objects from the API
            
        Returns:
            List of processed appointment objects with relevant fields
        """
        processed = []
        
        for appointment in appointments:
            # Extract data from the nested structure
            appt_data = appointment.get("appt", {})
            evt_data = appointment.get("evt", {})
            
            # Create a processed record with relevant fields
            processed_appt = {
                "id": appt_data.get("id"),
                "client_id": appt_data.get("clientId"),
                "client_name": appt_data.get("clientName"),
                "staff_id": appt_data.get("staffId"),
                "staff_name": appt_data.get("staffName"),
                "staff_title": appt_data.get("staffTitle"),
                "start_date": appt_data.get("startDate"),
                "start_time": self._convert_minutes_to_time(appt_data.get("startTime")),
                "end_time": self._convert_minutes_to_time(appt_data.get("endTime")),
                "status_id": appt_data.get("statusId"),
                "status_name": appt_data.get("statusName"),
                "cancellation_type_id": appt_data.get("cancellationTypeId"),
                "cancellation_note": appt_data.get("cancellationNote"),
                "location_name": appt_data.get("locationName"),
                "service_name": appt_data.get("serviceName"),
                "provider_service_name": appt_data.get("providerServiceName"),
                "funder_name": appt_data.get("funderName"),
                "date_created": appt_data.get("dateCreated"),
                "date_last_modified": appt_data.get("dateLastModified"),
                "modified_by": appt_data.get("modifiedBy"),
                "hours": evt_data.get("hours", 0),
                "minutes": evt_data.get("minutes", 0),
            }
            
            processed.append(processed_appt)
        
        return processed
    
    def _convert_minutes_to_time(self, minutes: Optional[int]) -> Optional[str]:
        """
        Convert minutes since midnight to a time string (HH:MM).
        
        Args:
            minutes: Minutes since midnight
            
        Returns:
            Time string in HH:MM format or None if minutes is None
        """
        if minutes is None:
            return None
            
        hours, mins = divmod(minutes, 60)
        return f"{hours:02d}:{mins:02d}"
    
    def get_cancelled_appointments(self, from_date_str: str, to_date_str: str) -> List[Dict[str, Any]]:
        """
        Main method to fetch and process cancelled appointments.
        
        Args:
            from_date_str: Start date string in ISO format (YYYY-MM-DD)
            to_date_str: End date string in ISO format (YYYY-MM-DD)
            
        Returns:
            List of processed cancelled appointment objects
            
        Raises:
            CancelledAppointmentsError: If date parameters are missing or invalid
        """
        logger.info("Starting cancelled appointments fetch", from_date=from_date_str, to_date=to_date_str)
        
        # Validate required parameters
        if not from_date_str:
            error_msg = "Missing required parameter: from_date_str must be provided in YYYY-MM-DD format"
            logger.error(error_msg)
            raise CancelledAppointmentsError(error_msg)
            
        if not to_date_str:
            error_msg = "Missing required parameter: to_date_str must be provided in YYYY-MM-DD format"
            logger.error(error_msg)
            raise CancelledAppointmentsError(error_msg)
        
        # Parse date strings to datetime objects
        try:
            from_date = parse_date(from_date_str).replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception as e:
            error_msg = f"Invalid from_date format: '{from_date_str}'. Expected YYYY-MM-DD format. Error: {str(e)}"
            logger.error(error_msg)
            raise CancelledAppointmentsError(error_msg)
            
        try:
            to_date = parse_date(to_date_str).replace(hour=23, minute=59, second=59, microsecond=999999)
        except Exception as e:
            error_msg = f"Invalid to_date format: '{to_date_str}'. Expected YYYY-MM-DD format. Error: {str(e)}"
            logger.error(error_msg)
            raise CancelledAppointmentsError(error_msg)
        
        # Check if date range exceeds 90 days (API limitation)
        date_diff = (to_date - from_date).days
        if date_diff < 0:
            error_msg = f"Invalid date range: from_date '{from_date_str}' must be before to_date '{to_date_str}'"
            logger.error(error_msg)
            raise CancelledAppointmentsError(error_msg)
            
        if date_diff > 90:
            logger.warning("Date range exceeds 90 days, which is the API limit. Splitting into batches.", 
                          date_diff=date_diff, from_date=from_date_str, to_date=to_date_str)
            return self._fetch_in_batches(from_date, to_date)
        
        # Fetch and process appointments
        raw_appointments = self.fetch_cancelled_appointments(from_date, to_date)
        processed_appointments = self.process_appointments(raw_appointments)
        
        logger.info("Completed cancelled appointments fetch", 
                   count=len(processed_appointments), 
                   from_date=from_date_str, 
                   to_date=to_date_str)
                   
        return processed_appointments
    
    def _fetch_in_batches(self, from_date: datetime.datetime, to_date: datetime.datetime) -> List[Dict[str, Any]]:
        """
        Fetch appointments in 90-day batches to handle API limitations.
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            Combined list of processed appointments from all batches
        """
        all_appointments = []
        current_from = from_date
        
        while current_from < to_date:
            # Calculate batch end date (90 days from current_from or to_date, whichever is earlier)
            batch_end = min(current_from + datetime.timedelta(days=90), to_date)
            
            logger.info(f"Fetching batch from {current_from} to {batch_end}")
            
            # Fetch this batch
            raw_batch = self.fetch_cancelled_appointments(current_from, batch_end)
            processed_batch = self.process_appointments(raw_batch)
            
            # Add to results
            all_appointments.extend(processed_batch)
            
            # Move to next batch
            current_from = batch_end + datetime.timedelta(days=1)
        
        return all_appointments
        
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
            raise CancelledAppointmentsError(f"Failed to get database URL: {str(e)}")

        raise CancelledAppointmentsError("Missing database URL")

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
            raise CancelledAppointmentsError(f"Database connection failed: {str(e)}")
            
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
                    raise CancelledAppointmentsError(f"Table '{self.table_name}' does not exist in the database. Please create the table first or check the table name.")

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
            raise CancelledAppointmentsError(f"Table '{self.table_name}' does not exist in the database. Please create the table first or check the table name.")
        except Exception as e:
            logger.error(f"Table truncation failed: {str(e)}")
            raise CancelledAppointmentsError(f"Table truncation failed: {str(e)}")
            
    def _prepare_row_data(self, appointment: Dict[str, Any]) -> list:
        """Prepare a single appointment record for database insertion."""
        # Define the columns in the order they should be inserted
        # Adjust these columns based on your database table structure
        db_columns = [
            'id', 'client_id', 'client_name', 'staff_id', 'staff_name',
            'staff_title', 'start_date', 'start_time', 'end_time',
            'status_id', 'status_name', 'cancellation_type_id',
            'cancellation_note', 'location_name', 'service_name',
            'provider_service_name', 'funder_name', 'date_created',
            'date_last_modified', 'modified_by', 'hours', 'minutes'
        ]
        
        values = []
        for col in db_columns:
            # Get the value from the appointment dict, or None if not present
            value = appointment.get(col)
            values.append(value)
            
        return values
        
    def _insert_data(self, conn, appointments: List[Dict[str, Any]]) -> tuple[int, int]:
        """Insert appointment data into database using batch operations."""
        logger.info("Starting data insertion")

        success_count = 0
        error_count = 0

        # Prepare all rows for batch insertion
        all_values = []
        for i, appointment in enumerate(appointments):
            try:
                values = self._prepare_row_data(appointment)
                all_values.append(values)
            except Exception as e:
                error_count += 1
                logger.warning(f"Error preparing appointment record {i + 1}: {e}")

        # Batch size for insertion
        batch_size = 50  # Smaller batch size for complex data

        try:
            with conn.cursor() as cur:
                for i in range(0, len(all_values), batch_size):
                    batch = all_values[i:i+batch_size]
                    
                    if not batch:  # Skip empty batches
                        continue

                    # Count the number of values in each row to create the correct number of placeholders
                    num_values = len(batch[0])
                    placeholders = ','.join(['(%s)' % ','.join(['%s'] * num_values)] * len(batch))
                    
                    # Flatten the batch values for the execute call
                    flattened_values = [item for sublist in batch for item in sublist]

                    # Define the columns based on the database table structure
                    columns = [
                        'id', 'client_id', 'client_name', 'staff_id', 'staff_name',
                        'staff_title', 'start_date', 'start_time', 'end_time',
                        'status_id', 'status_name', 'cancellation_type_id',
                        'cancellation_note', 'location_name', 'service_name',
                        'provider_service_name', 'funder_name', 'date_created',
                        'date_last_modified', 'modified_by', 'hours', 'minutes'
                    ]
                    
                    column_str = '", "'.join(columns)
                    
                    query = f"""
                        INSERT INTO "{self.table_name}" ("{column_str}")
                        VALUES {placeholders}
                    """

                    cur.execute(query, flattened_values)
                    success_count += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1}, records {i+1}-{i+len(batch)}")

                conn.commit()
                logger.info(f"Data insertion completed: {success_count} success, {error_count} errors")

        except Exception as e:
            conn.rollback()
            logger.error(f"Data insertion failed: {e}")
            raise CancelledAppointmentsError(f"Data insertion failed: {e}")
        
        return success_count, error_count


    @log_performance
    def sync_to_database(self, from_date_str: str, to_date_str: str, truncate: bool = True) -> Dict[str, Any]:
        """
        Fetch cancelled appointments and sync them to the database.
        
        Args:
            from_date_str: Start date string in ISO format (YYYY-MM-DD)
            to_date_str: End date string in ISO format (YYYY-MM-DD)
            truncate: Whether to truncate the table before inserting data
            
        Returns:
            Dictionary containing sync results and statistics
            
        Raises:
            CancelledAppointmentsError: If parameters are missing or invalid
        """
        # Generate a sync ID for tracking this operation
        start_time = time.time()
        
        # Log sync start
        sync_id = sync_logger.log_sync_start("cancelled_appointments", {
            "table_name": self.table_name,
            "from_date": from_date_str,
            "to_date": to_date_str,
            "truncate": truncate
        })
        
        # Validate parameters
        if truncate is None:
            error_msg = "Missing required parameter: truncate must be explicitly set to True or False"
            logger.error(error_msg)
            sync_logger.log_sync_error(sync_id, error_msg)
            raise CancelledAppointmentsError(error_msg)
            
        if not isinstance(truncate, bool):
            error_msg = f"Invalid truncate parameter: expected bool, got {type(truncate).__name__}"
            logger.error(error_msg)
            sync_logger.log_sync_error(sync_id, error_msg)
            raise CancelledAppointmentsError(error_msg)
        
        try:
            # Fetch cancelled appointments - this will validate date parameters
            logger.info(f"Fetching cancelled appointments for sync", 
                       from_date=from_date_str, 
                       to_date=to_date_str)
                       
            appointments = self.get_cancelled_appointments(from_date_str, to_date_str)
            
            if not appointments:
                logger.warning("No cancelled appointments found to sync")
                result = {
                    "status": "success",
                    "sync_message": "No data to sync",
                    "records_processed": 0,
                    "records_inserted": 0,
                    "errors": 0,
                    "table_name": self.table_name,
                    "duration_seconds": round(time.time() - start_time, 2)
                }
                sync_logger.log_sync_complete(sync_id, result)
                return result
            
            # Connect to database
            logger.info("Connecting to database")
            db_url = self._get_database_url()
            conn = self._connect_database(db_url)
            
            # Truncate table if requested
            if truncate:
                logger.info(f"Truncating table '{self.table_name}'")
                self._truncate_table(conn)
                logger.info(f"Table {self.table_name} truncated successfully")
            else:
                logger.info(f"Skipping table truncation for '{self.table_name}'")
            
            # Insert data
            logger.info(f"Inserting {len(appointments)} records into '{self.table_name}'")
            success_count, error_count = self._insert_data(conn, appointments)
            
            # Close connection
            conn.close()
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Prepare result
            result = {
                "status": "success",
                "sync_message": f"Successfully synced {success_count} cancelled appointments to {self.table_name}",
                "table_name": self.table_name,
                "records_processed": len(appointments),
                "records_inserted": success_count,
                "errors": error_count,
                "duration_seconds": round(duration, 2)
            }
            
            # Log completion
            sync_logger.log_sync_complete(sync_id, result)
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            sync_logger.log_sync_error(sync_id, error_msg)
            
            return {
                "status": "error",
                "sync_message": error_msg,
                "table_name": self.table_name,
                "duration_seconds": round(duration, 2)
            }


def fetch_cancelled_appointments(from_date: str, to_date: str) -> List[Dict[str, Any]]:
    """
    Convenience function to fetch cancelled appointments without creating a class instance.
    
    Args:
        from_date: Start date string in ISO format (YYYY-MM-DD)
        to_date: End date string in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed cancelled appointment objects
    """
    fetcher = CancelledAppointmentsFetcher()
    return fetcher.get_cancelled_appointments(from_date, to_date)
    
    
def sync_cancelled_appointments_to_database(from_date: str, to_date: str, table_name: str, truncate: bool) -> Dict[str, Any]:
    """
    Convenience function to sync cancelled appointments to a database table without creating a class instance.
    
    Args:
        from_date: Start date string in ISO format (YYYY-MM-DD), required
        to_date: End date string in ISO format (YYYY-MM-DD), required
        table_name: Name of the database table to insert data into, required
        truncate: Whether to truncate the table before inserting data, required
        
    Returns:
        Dictionary containing sync results and statistics
        
    Raises:
        CancelledAppointmentsError: If parameters are missing or invalid
    """
    # Validate parameters
    if not table_name:
        error_msg = "Missing required parameter: table_name must be provided"
        logger.error(error_msg)
        raise CancelledAppointmentsError(error_msg)
        
    if truncate is None:
        error_msg = "Missing required parameter: truncate must be explicitly set to True or False"
        logger.error(error_msg)
        raise CancelledAppointmentsError(error_msg)
    
    # Create fetcher and sync
    fetcher = CancelledAppointmentsFetcher(auth=None, table_name=table_name)
    return fetcher.sync_to_database(from_date, to_date, truncate)
