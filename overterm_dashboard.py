#!/usr/bin/env python3
"""
Over Term Dashboard module for Rethink BH API.
Handles fetching dashboard data for clients with Over Term authorization status.
"""

import logging
import os
import psycopg2
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import urlparse
from auth import RethinkAuth, RethinkAuthError

# Configure logging
logger = logging.getLogger(__name__)

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
        
    def get_dashboard_data(
        self,
        start_date: str = None,
        end_date: str = None,
        client_ids: List[int] = None
    ) -> Dict[str, Any]:
        """Fetch Over Term dashboard data for ABA clients."""
        logger.info("Fetching Over Term dashboard data")

        # Set defaults
        start_date = start_date or "07/01/2024"
        end_date = end_date or "07/31/2025"
        client_ids = client_ids or [325526, 349284, 304907, 297808]

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
                    "clients_found": len(set(detail.get("ClientName", "") for detail in auth_details)),
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

    def _truncate_overterm_table(self, conn, table_name: str = "overterm_dashboard") -> None:
        """Truncate the Over Term dashboard table and reset ID sequence."""
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

                # Truncate the table
                cur.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
                logger.info(f"Table '{table_name}' truncated")

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
            logger.error(f"Table truncation failed: {e}")
            raise OverTermDashboardError(f"Table truncation failed: {e}")

    def _prepare_authorization_data(self, auth_detail: Dict[str, Any]) -> List[Any]:
        """Prepare a single authorization record for database insertion."""
        # Map API response fields to database columns
        return [
            auth_detail.get("ClientName"),
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
            auth_detail.get("RenderingProvider"),
            auth_detail.get("ProcedureCodeId"),
            auth_detail.get("ReferringProviderName")
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
                # Truncate existing data
                self._truncate_overterm_table(conn, table_name)

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


