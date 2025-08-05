#!/usr/bin/env python3
"""
Over Term Dashboard module for Rethink BH API.
Handles fetching dashboard data for clients with Over Term authorization status.
"""

import os
import psycopg2
import re
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import urlparse
from google.cloud import secretmanager

from config import config
from logger import get_logger, get_sync_logger, log_performance
from auth import RethinkAuth, RethinkAuthError

# Initialize logging
logger = get_logger(__name__)
sync_logger = get_sync_logger(logger)

class OverTermDashboardError(Exception):
    """Custom exception for Over Term dashboard operations."""
    pass

class OverTermDashboard:
    """Handles Over Term dashboard data fetching from Rethink BH API."""

    def __init__(self, auth: RethinkAuth = None):
        """
        Initialize Over Term Dashboard.

        Args:
            auth: Optional RethinkAuth instance. If not provided, creates a new one.
        """
        self.auth = auth or RethinkAuth()

    def _generate_name_code(self, full_name: str) -> str:
        """
        Generate a 4-character name code from a full name for privacy/compression.

        For the overterm dashboard, client names are in "Lastname, Firstname" format.
        The nameCode should always be FiLa format (first two letters of first name + first two of last name).

        Format: FirstNameInitials + LastNameInitials (mixed case)
        - Takes first 2 letters of first name + first 2 letters of last name
        - Capitalizes first letter of each pair, lowercase for second letter
        - Removes special characters (apostrophes, hyphens, etc.)
        - Handles nicknames in parentheses by ignoring them

        Examples:
        - "Doe, John (Jane)" -> "JoDo"
        - "O'Connor, Patrick" -> "PaOc"
        - "Smith-Jones, Mary-Ann" -> "MaSm"

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
                    return single_code.capitalize() + single_code[1:].lower()[:1] + \
                           single_code[2:].capitalize()[:1] + single_code[3:].lower()[:1]

            # Clean names by removing special characters and keeping only letters
            first_name_clean = re.sub(r'[^a-zA-Z]', '', first_name)
            last_name_clean = re.sub(r'[^a-zA-Z]', '', last_name)

            # Extract first 2 characters from cleaned first name and capitalize properly
            first_code = first_name_clean[:2] if len(first_name_clean) >= 2 else (first_name_clean + "X")[:2]
            first_code = first_code[0].upper() + first_code[1].lower()  # First letter uppercase, second lowercase

            # Extract first 2 characters from cleaned last name and capitalize properly
            last_code = last_name_clean[:2] if len(last_name_clean) >= 2 else (last_name_clean + "X")[:2]
            last_code = last_code[0].upper() + last_code[1].lower()  # First letter uppercase, second lowercase

            # Combine first name code + last name code
            name_code = first_code + last_code

            logger.debug(f"Generated name code '{name_code}' for '{full_name}'")
            return name_code

        except Exception as e:
            logger.warning(f"Error generating name code for '{full_name}': {e}")
            return "UnKn"
        
    @log_performance
    def get_dashboard_data(
        self,
        start_date: str = None,
        end_date: str = None,
        client_ids: List[int] = None
    ) -> Dict[str, Any]:
        """Fetch Over Term dashboard data for ABA clients."""
        logger.info("Fetching Over Term dashboard data")

        # Validate required parameters
        if not start_date:
            error_msg = "Missing required parameter: start_date must be provided in MM/dd/yyyy format"
            logger.error(error_msg)
            raise OverTermDashboardError(error_msg)

        if not end_date:
            error_msg = "Missing required parameter: end_date must be provided in MM/dd/yyyy format"
            logger.error(error_msg)
            raise OverTermDashboardError(error_msg)

        # Handle client_ids properly - require explicit specification
        if client_ids is None:
            error_msg = "Missing required parameter: client_ids must be provided as a list of integers (use empty list [] for all clients)"
            logger.error(error_msg)
            raise OverTermDashboardError(error_msg)
        # If empty list [] is passed, keep it as empty (means all clients)
        # If list with IDs is passed, use those specific IDs

        payload = {
            "reportId": 13,
            "printMode": 0,
            "filters": {
                "AuthorizationStatus": [1],  # Over Term
                "ServiceLines": [6476],      # ABA
                "Locations": [],
                "Funders": [],
                "Clients": client_ids,
                "ClientsInit": [],
                "ClientStatus": [1],         # Active
                "StartDate": start_date,
                "EndDate": end_date,
                "IncludeDemoClients": False,
                "ClientDateFormat": "MM/dd/yyyy"
            },
            "clientDateFormat": "MM/dd/yyyy"
        }

        try:
            response = self.auth.make_request(
                "POST",
                f"{self.auth.base_url}/HealthCare/ReportingDashboard/GetDashboardReport",
                request_type="dashboard",
                json=payload,
                timeout=120
            )

            dashboard_data = response.json()
            auth_details = dashboard_data.get("Reports", {}).get("ReportData", {}).get("AuthorizationUtilizationDetails", [])

            logger.info(f"Retrieved {len(auth_details)} authorization records")

            return {
                "status": "success",
                "data": dashboard_data,
                "filters_applied": payload["filters"],
                "summary": {
                    "total_authorizations": len(auth_details),
                    "clients_found": len(set(self._generate_name_code(detail.get("ClientName", "")) for detail in auth_details)),
                    "unique_client_codes": list(set(self._generate_name_code(detail.get("ClientName", "")) for detail in auth_details)),
                    "date_range": f"{start_date} to {end_date}"
                },
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Dashboard request failed: {e}")
            raise OverTermDashboardError(f"Dashboard request failed: {e}")

    def _get_database_url(self) -> str:
        """Get database URL from environment or Secret Manager."""
        db_url = os.getenv("SUPABASE_DB_URL")

        if db_url:
            logger.debug("Using database URL from environment variables")
            return db_url

        # If not found, try Secret Manager
        try:
            db_url = self.auth._get_secret("SUPABASE_DB_URL")
            if db_url:
                return db_url
        except RethinkAuthError as e:
            raise OverTermDashboardError(f"Failed to get database URL: {e}")

        raise OverTermDashboardError("Missing database URL")

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
            logger.debug("Connected to Supabase database")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise OverTermDashboardError(f"Database connection failed: {e}")
            
    def _reset_id_sequence(self, conn, table_name: str = "overterm_dashboard") -> None:
        """Reset the ID sequence for the table without truncating data."""
        try:
            with conn.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND lower(table_name) = lower(%s)
                    )
                """, (table_name,))
                table_exists = cur.fetchone()[0]

                if not table_exists:
                    raise OverTermDashboardError(f"Table '{table_name}' does not exist")

                # Reset sequence
                cur.execute(f"""
                    SELECT pg_get_serial_sequence('"{table_name}"', 'id')
                """)
                result = cur.fetchone()
                seq_name = result[0] if result else None

                if seq_name:
                    cur.execute(f"SELECT setval('{seq_name}', 1, false)")
                    logger.debug(f"Reset sequence {seq_name}")

                conn.commit()
        except Exception as e:
            logger.error(f"Sequence reset failed: {e}")
            raise OverTermDashboardError(f"Sequence reset failed: {e}")

    def _prepare_authorization_data(self, auth_detail: Dict[str, Any]) -> List[Any]:
        """
        Prepare a single authorization record for database insertion.

        Converts full names to name codes for privacy/compression:
        - ClientName: Full name -> 4-character code (e.g., "Gayagoy, Celine" -> "CeGa")
        - RenderingProvider: Full name -> 4-character code
        - ReferringProviderName: Full name -> 4-character code
        """
        # Generate name codes for name fields
        client_name_code = self._generate_name_code(auth_detail.get("ClientName"))
        rendering_provider_code = self._generate_name_code(auth_detail.get("RenderingProvider"))
        referring_provider_code = self._generate_name_code(auth_detail.get("ReferringProviderName"))

        # Map API response fields to database columns
        return [
            client_name_code,  # Use name code instead of full ClientName
            auth_detail.get("FunderName"),
            auth_detail.get("ServiceLine"),
            auth_detail.get("AuthorizationNumber"),
            auth_detail.get("AuthorizationUnit"),
            auth_detail.get("Dates"),
            auth_detail.get("BillingCodes"),
            auth_detail.get("ServiceName"),
            auth_detail.get("BillCode"),
            auth_detail.get("SchedulingGoal"),
            auth_detail.get("TotalSchedGoal"),
            auth_detail.get("TotalAuthHours"),
            auth_detail.get("SchedHours"),
            auth_detail.get("UnschedHours"),
            auth_detail.get("VerifiedHours"),
            auth_detail.get("SchedAuth"),
            auth_detail.get("SchedGoal"),
            auth_detail.get("DaysUntilExpiration"),
            auth_detail.get("AuthorizationStatus"),
            rendering_provider_code,  # Use name code instead of full RenderingProvider
            auth_detail.get("ProcedureCodeId"),
            referring_provider_code   # Use name code instead of full ReferringProviderName
        ]

    def _insert_overterm_data(self, conn, auth_details: List[Dict[str, Any]], table_name: str = "overterm_dashboard") -> tuple[int, int]:
        """Insert Over Term authorization data into database using batch operations."""
        logger.info(f"Starting insertion of {len(auth_details)} authorization records")

        success_count = 0
        error_count = 0

        # Prepare all rows for batch insertion
        all_values = []
        for i, auth_detail in enumerate(auth_details):
            try:
                values = self._prepare_authorization_data(auth_detail)
                all_values.append(values)
            except Exception as e:
                error_count += 1
                logger.warning(f"Error preparing authorization record {i + 1}: {e}")

        # Batch size for insertion
        batch_size = 50  # Smaller batch size for complex data

        try:
            with conn.cursor() as cur:
                for i in range(0, len(all_values), batch_size):
                    batch = all_values[i:i+batch_size]

                    # Construct batch insert query
                    placeholders = ','.join(['(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'] * len(batch))
                    flattened_values = [item for sublist in batch for item in sublist]

                    query = f"""
                        INSERT INTO "{table_name}" (
                            clientname, fundername, serviceline, authorizationnumber,
                            authorizationunit, dates, billingcodes, servicename,
                            billcode, schedulinggoal, totalschedgoal, totalauthhours,
                            schedhours, unschedhours, verifiedhours, schedauth,
                            schedgoal, daysuntilexpiration, authorizationstatus,
                            renderingprovider, procedurecodeid, referringprovidername
                        ) VALUES {placeholders}
                    """

                    cur.execute(query, flattened_values)
                    success_count += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1}, records {i+1}-{i+len(batch)}")

                conn.commit()
                logger.info(f"Data insertion completed: {success_count} success, {error_count} errors")

        except Exception as e:
            conn.rollback()
            logger.error(f"Data insertion failed: {e}")
            raise OverTermDashboardError(f"Data insertion failed: {e}")

        return success_count, error_count

    @log_performance
    def sync_to_database(
        self,
        start_date: str = None,
        end_date: str = None,
        client_ids: List[int] = None,
        table_name: str = "overterm_dashboard"
    ) -> Dict[str, Any]:
        """
        Fetch Over Term dashboard data and sync it to Supabase database.

        Args:
            start_date: Start date in MM/dd/yyyy format
            end_date: End date in MM/dd/yyyy format
            client_ids: List of client IDs to filter for
            table_name: Database table name (default: overterm_dashboard)

        Returns:
            Dictionary containing sync results and statistics
        """
        logger.info("Starting Over Term dashboard sync to database")

        try:
            # Fetch dashboard data
            dashboard_result = self.get_dashboard_data(start_date, end_date, client_ids)

            if dashboard_result["status"] != "success":
                raise OverTermDashboardError("Failed to fetch dashboard data")

            auth_details = dashboard_result["data"].get("Reports", {}).get("ReportData", {}).get("AuthorizationUtilizationDetails", [])

            if not auth_details:
                logger.warning("No Over Term authorization data found to sync")
                return {
                    "status": "success",
                    "message": "No data to sync",
                    "records_processed": 0,
                    "records_inserted": 0,
                    "errors": 0,
                    "filters_applied": dashboard_result["filters_applied"],
                    "timestamp": datetime.now().isoformat()
                }

            # Connect to database and sync data
            db_url = self._get_database_url()

            with self._connect_database(db_url) as conn:
                # No longer truncating existing data - sequential requests will append data
                # Reset ID sequence before insertion
                self._reset_id_sequence(conn, table_name)
                
                # Insert new data
                success_count, error_count = self._insert_overterm_data(conn, auth_details, table_name)

                logger.info(f"Sync completed: {len(auth_details)} records processed, {success_count} inserted, {error_count} errors")

                return {
                    "status": "success",
                    "message": f"Synced {success_count} Over Term authorization records",
                    "table": table_name,
                    "records_processed": len(auth_details),
                    "records_inserted": success_count,
                    "errors": error_count,
                    "filters_applied": dashboard_result["filters_applied"],
                    "summary": dashboard_result["summary"],
                    "timestamp": datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Over Term dashboard sync failed: {e}")
            raise OverTermDashboardError(f"Sync failed: {e}")


