"""
Database Connector Module

Provides functionality to connect to and interact with SQL Server databases,
including the PACS TrueAutomation database and the DataBridge's own database.
"""

import os
import logging
import pyodbc
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    """SQL Server database connector for the PACS Data Bridge system."""
    
    def __init__(
        self, 
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: bool = False
    ):
        """
        Initialize the database connector.
        
        Args:
            server: SQL Server name/address
            database: Database name
            username: SQL Server username (if not using trusted connection)
            password: SQL Server password (if not using trusted connection)
            trusted_connection: Whether to use Windows authentication
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to the SQL Server database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Build connection string
            if self.trusted_connection:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                )
                
            # Establish connection
            self.connection = pyodbc.connect(conn_str)
            logger.info(f"Successfully connected to {self.database} on {self.server}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            return False
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                logger.info(f"Disconnected from {self.database}")
            except Exception as e:
                logger.error(f"Error disconnecting from database: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Tuple]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Parameters for parameterized queries
            
        Returns:
            List of result tuples or None if error
        """
        if not self.connection:
            if not self.connect():
                return None
                
        try:
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            # Fetch results if this is a SELECT query
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                self.connection.commit()
                cursor.close()
                return []
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            return None
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> bool:
        """
        Execute a SQL query with multiple parameter sets (batch insert/update).
        
        Args:
            query: SQL query template
            params_list: List of parameter tuples
            
        Returns:
            True if successful, False otherwise
        """
        if not self.connection:
            if not self.connect():
                return False
                
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params_list)
            self.connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Error executing batch query: {str(e)}")
            return False
    
    def query_to_dataframe(self, query: str, params: Optional[Tuple] = None) -> Optional[pd.DataFrame]:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query to execute
            params: Parameters for parameterized queries
            
        Returns:
            Pandas DataFrame with query results or None if error
        """
        if not self.connection:
            if not self.connect():
                return None
                
        try:
            if params:
                df = pd.read_sql(query, self.connection, params=params)
            else:
                df = pd.read_sql(query, self.connection)
                
            return df
            
        except Exception as e:
            logger.error(f"Error executing query to DataFrame: {str(e)}")
            return None
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column information dictionaries or None if error
        """
        # Query to get column information
        query = """
        SELECT 
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            ISNULL(ep.value, '') AS description
        FROM 
            sys.columns c
        JOIN 
            sys.types t ON c.user_type_id = t.user_type_id
        LEFT JOIN 
            sys.extended_properties ep ON ep.major_id = c.object_id 
                AND ep.minor_id = c.column_id 
                AND ep.name = 'MS_Description'
        WHERE 
            c.object_id = OBJECT_ID(?)
        ORDER BY 
            c.column_id
        """
        
        try:
            results = self.execute_query(query, (table_name,))
            
            if results is None:
                return None
                
            columns = []
            for row in results:
                column = {
                    'name': row[0],
                    'data_type': row[1],
                    'max_length': row[2],
                    'precision': row[3],
                    'scale': row[4],
                    'is_nullable': bool(row[5]),
                    'is_identity': bool(row[6]),
                    'description': row[7]
                }
                columns.append(column)
                
            return columns
            
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            return None


class PACSConnector(DatabaseConnector):
    """Specialized connector for the PACS TrueAutomation database."""
    
    def get_parcel_by_address(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """
        Search for parcels by address using fuzzy matching.
        
        Args:
            address: Address to search for
            
        Returns:
            List of matching parcels or None if error
        """
        # Create a simplified address for searching
        search_address = '%' + address.replace(' ', '%') + '%'
        
        # Query to find matching parcels
        query = """
        SELECT TOP 10
            p.pid,
            p.situs_num as street_number,
            p.situs_street_name as street_name,
            p.situs_city as city,
            p.situs_state as state,
            p.situs_zip as zip,
            CONCAT(p.situs_num, ' ', p.situs_street_name, ', ', p.situs_city, ', ', p.situs_state) as full_address
        FROM 
            property p
        WHERE 
            CONCAT(p.situs_num, ' ', p.situs_street_name, ', ', p.situs_city, ', ', p.situs_state) LIKE ?
        ORDER BY
            p.situs_num, p.situs_street_name
        """
        
        try:
            df = self.query_to_dataframe(query, (search_address,))
            
            if df is None or df.empty:
                return []
                
            return df.to_dict(orient='records')
            
        except Exception as e:
            logger.error(f"Error searching for parcel by address: {str(e)}")
            return None
    
    def get_parcel_by_parcel_number(self, parcel_number: str) -> Optional[Dict[str, Any]]:
        """
        Get parcel information by parcel number.
        
        Args:
            parcel_number: Parcel number to look up
            
        Returns:
            Parcel information or None if not found or error
        """
        query = """
        SELECT TOP 1
            p.pid,
            p.situs_num as street_number,
            p.situs_street_name as street_name,
            p.situs_city as city,
            p.situs_state as state,
            p.situs_zip as zip,
            CONCAT(p.situs_num, ' ', p.situs_street_name, ', ', p.situs_city, ', ', p.situs_state) as full_address
        FROM 
            property p
        WHERE 
            p.pid = ?
        """
        
        try:
            df = self.query_to_dataframe(query, (parcel_number,))
            
            if df is None or df.empty:
                return None
                
            return df.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"Error getting parcel by number: {str(e)}")
            return None


# Example usage
if __name__ == "__main__":
    # This would be replaced with proper configuration in production
    # Using trusted connection for local testing
    connector = PACSConnector(
        server="localhost",
        database="PACS",
        trusted_connection=True
    )
    
    # Test connection
    if connector.connect():
        print("Successfully connected to PACS database")
        
        # Test address search
        results = connector.get_parcel_by_address("123 Main")
        if results:
            print(f"Found {len(results)} matching parcels:")
            for parcel in results:
                print(f"  {parcel['full_address']} (PID: {parcel['pid']})")
        else:
            print("No matching parcels found")
            
        # Disconnect
        connector.disconnect()
    else:
        print("Failed to connect to PACS database")
"""
Database connection module for PACS TrueAutomation and DataBridge databases.
"""

import pyodbc
import pandas as pd
from typing import Dict, List, Optional, Union, Any, Tuple
import logging

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """
    Database connector for SQL Server databases.
    Handles connections to both the PACS TrueAutomation database and the DataBridge database.
    """
    
    def __init__(
        self,
        server: str,
        database: str,
        username: str = '',
        password: str = '',
        trusted_connection: bool = True,
        timeout: int = 30
    ):
        """
        Initialize a database connector.
        
        Args:
            server: SQL Server name/address
            database: Database name
            username: SQL username (if not using Windows authentication)
            password: SQL password (if not using Windows authentication)
            trusted_connection: Use Windows authentication
            timeout: Connection timeout in seconds
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.timeout = timeout
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Connect to the database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Build connection string
            if self.trusted_connection:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"Trusted_Connection=yes;"
                    f"timeout={self.timeout};"
                )
            else:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"timeout={self.timeout};"
                )
            
            # Establish connection
            self.connection = pyodbc.connect(conn_str)
            self.cursor = self.connection.cursor()
            
            logger.info(f"Connected to {self.database} on {self.server}")
            return True
            
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            self.connection = None
            self.cursor = None
            return False
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            try:
                if self.cursor:
                    self.cursor.close()
                self.connection.close()
                logger.info(f"Disconnected from {self.database}")
            except Exception as e:
                logger.error(f"Error disconnecting from database: {str(e)}")
            
            self.connection = None
            self.cursor = None
    
    def is_connected(self) -> bool:
        """
        Check if database is connected.
        
        Returns:
            True if connected, False otherwise
        """
        if not self.connection or not self.cursor:
            return False
        
        try:
            # Try a simple query to check connection
            self.cursor.execute("SELECT 1")
            return True
        except:
            return False
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None, 
        commit: bool = True
    ) -> Optional[List[Tuple]]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query string
            params: Query parameters (for parameterized queries)
            commit: Whether to commit the transaction
        
        Returns:
            Query results as a list of tuples, or None for non-query statements
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            # Try to fetch results (will fail for non-SELECT queries)
            try:
                results = self.cursor.fetchall()
                return [tuple(row) for row in results]
            except pyodbc.Error:
                # Not a query that returns results
                if commit:
                    self.connection.commit()
                return None
                
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            return None
    
    def insert_many(self, table: str, columns: List[str], data: List[Tuple]) -> bool:
        """
        Insert multiple rows into a table.
        
        Args:
            table: Table name
            columns: Column names
            data: List of data tuples to insert
        
        Returns:
            True if successful, False otherwise
        """
        if not data:
            logger.warning("No data provided for insert_many")
            return False
        
        if not self.is_connected():
            if not self.connect():
                return False
        
        try:
            # Create placeholders string
            placeholders = ','.join(['?' for _ in columns])
            
            # Create SQL query
            columns_str = ','.join(columns)
            query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
            
            # Execute many
            self.cursor.executemany(query, data)
            self.connection.commit()
            
            logger.info(f"Inserted {len(data)} rows into {table}")
            return True
            
        except Exception as e:
            logger.error(f"Bulk insert error: {str(e)}")
            return False
    
    def execute_stored_procedure(
        self, 
        procedure: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Tuple]]:
        """
        Execute a stored procedure.
        
        Args:
            procedure: Stored procedure name
            params: Named parameters for the procedure
        
        Returns:
            Results as a list of tuples, or None on error
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Build the SQL call with named parameters
            if params:
                param_str = ', '.join([f"@{name}=?" for name in params])
                call_str = f"{{CALL {procedure}({param_str})}}"
                self.cursor.execute(call_str, list(params.values()))
            else:
                call_str = f"{{CALL {procedure}}}"
                self.cursor.execute(call_str)
            
            # Try to fetch results
            try:
                results = self.cursor.fetchall()
                return [tuple(row) for row in results]
            except pyodbc.Error:
                # Not a procedure that returns results
                self.connection.commit()
                return None
                
        except Exception as e:
            logger.error(f"Stored procedure execution error: {str(e)}")
            logger.error(f"Procedure: {procedure}")
            if params:
                logger.error(f"Parameters: {params}")
            return None
    
    def query_to_dataframe(self, query: str, params: Optional[Tuple] = None) -> Optional[pd.DataFrame]:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
        
        Returns:
            DataFrame with query results, or None on error
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            if params:
                return pd.read_sql(query, self.connection, params=params)
            else:
                return pd.read_sql(query, self.connection)
                
        except Exception as e:
            logger.error(f"DataFrame query error: {str(e)}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            return None
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Table name to check
        
        Returns:
            True if the table exists, False otherwise
        """
        if not self.is_connected():
            if not self.connect():
                return False
        
        try:
            # Query to check if table exists
            query = f"""
            SELECT OBJECT_ID('{table_name}', 'U') AS table_id
            """
            
            result = self.execute_query(query)
            
            if result and result[0][0]:
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            return False


class PACSConnector(DatabaseConnector):
    """
    Specialized connector for the PACS TrueAutomation database with PACS-specific methods.
    """
    
    def __init__(
        self,
        server: str,
        database: str = 'PACS',
        username: str = '',
        password: str = '',
        trusted_connection: bool = True,
        timeout: int = 30
    ):
        """Initialize PACS connector with default database name."""
        super().__init__(server, database, username, password, trusted_connection, timeout)
    
    def get_parcel_by_address(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Find a parcel by address.
        
        Args:
            address: Address to search for
        
        Returns:
            Parcel information or None if not found
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Extract address components for better searching
            address = address.strip().upper()
            
            # Query to find parcels by address
            query = """
            SELECT TOP 1
                p.ParcelID,
                p.ParcelNumber,
                p.SitusAddress,
                p.SitusCity,
                p.SitusState,
                p.SitusZip,
                o.OwnerName
            FROM 
                Parcels p
                LEFT JOIN ParcelOwners o ON p.ParcelID = o.ParcelID AND o.IsPrimary = 1
            WHERE 
                p.SitusAddress LIKE ?
            ORDER BY
                p.ParcelNumber ASC
            """
            
            # Use wildcard search
            params = (f"%{address}%",)
            
            result = self.execute_query(query, params)
            
            if result and len(result) > 0:
                row = result[0]
                return {
                    'parcel_id': row[0],
                    'parcel_number': row[1],
                    'address': row[2],
                    'city': row[3],
                    'state': row[4],
                    'zip': row[5],
                    'owner_name': row[6]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching for parcel by address: {str(e)}")
            return None
    
    def get_parcel_by_number(self, parcel_number: str) -> Optional[Dict[str, Any]]:
        """
        Find a parcel by parcel number.
        
        Args:
            parcel_number: Parcel number to search for
        
        Returns:
            Parcel information or None if not found
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Clean up parcel number
            parcel_number = parcel_number.strip().upper()
            parcel_number = parcel_number.replace('-', '')
            
            # Query to find parcel by number
            query = """
            SELECT TOP 1
                p.ParcelID,
                p.ParcelNumber,
                p.SitusAddress,
                p.SitusCity,
                p.SitusState,
                p.SitusZip,
                o.OwnerName
            FROM 
                Parcels p
                LEFT JOIN ParcelOwners o ON p.ParcelID = o.ParcelID AND o.IsPrimary = 1
            WHERE 
                p.ParcelNumber LIKE ?
            """
            
            # Exact match with formatting variations
            parcel_formatted = parcel_number
            params = (f"%{parcel_formatted}%",)
            
            result = self.execute_query(query, params)
            
            if result and len(result) > 0:
                row = result[0]
                return {
                    'parcel_id': row[0],
                    'parcel_number': row[1],
                    'address': row[2],
                    'city': row[3],
                    'state': row[4],
                    'zip': row[5],
                    'owner_name': row[6]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching for parcel by number: {str(e)}")
            return None
    
    def get_permits(
        self, 
        limit: int = 100,
        offset: int = 0
    ) -> Optional[pd.DataFrame]:
        """
        Get permits from PACS database.
        
        Args:
            limit: Maximum number of permits to return
            offset: Number of permits to skip
        
        Returns:
            DataFrame with permits or None on error
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Query to get permits
            query = f"""
            SELECT 
                PermitID,
                PermitNumber,
                IssuedDate,
                PermitType,
                Description,
                ProjectAddress,
                ParcelID,
                ParcelNumber,
                Valuation,
                Status
            FROM 
                Permits
            ORDER BY
                IssuedDate DESC
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY
            """
            
            return self.query_to_dataframe(query)
            
        except Exception as e:
            logger.error(f"Error getting permits: {str(e)}")
            return None
    
    def get_personal_property(
        self, 
        limit: int = 100,
        offset: int = 0
    ) -> Optional[pd.DataFrame]:
        """
        Get personal property accounts from PACS database.
        
        Args:
            limit: Maximum number of accounts to return
            offset: Number of accounts to skip
        
        Returns:
            DataFrame with personal property accounts or None on error
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Query to get personal property accounts
            query = f"""
            SELECT 
                pp.PersonalPropertyID,
                pp.AccountNumber,
                pp.BusinessName,
                pp.OwnerName,
                pp.SitusAddress,
                pp.SitusCity,
                pp.SitusState,
                pp.SitusZip,
                pp.MailingAddress,
                pp.MailingCity,
                pp.MailingState,
                pp.MailingZip,
                pp.PropertyType,
                ppa.AssessmentYear,
                ppa.AssessedValue
            FROM 
                PersonalProperty pp
                LEFT JOIN PersonalPropertyAssessments ppa ON pp.PersonalPropertyID = ppa.PersonalPropertyID
                    AND ppa.AssessmentYear = (SELECT MAX(AssessmentYear) FROM PersonalPropertyAssessments)
            ORDER BY
                pp.AccountNumber
            OFFSET {offset} ROWS
            FETCH NEXT {limit} ROWS ONLY
            """
            
            return self.query_to_dataframe(query)
            
        except Exception as e:
            logger.error(f"Error getting personal property accounts: {str(e)}")
            return None


class DataBridgeConnector(DatabaseConnector):
    """
    Specialized connector for the DataBridge database with DataBridge-specific methods.
    """
    
    def __init__(
        self,
        server: str,
        database: str = 'DataBridge',
        username: str = '',
        password: str = '',
        trusted_connection: bool = True,
        timeout: int = 30
    ):
        """Initialize DataBridge connector with default database name."""
        super().__init__(server, database, username, password, trusted_connection, timeout)
    
    def get_config_settings(self, category: Optional[str] = None) -> Optional[Dict[str, Dict[str, str]]]:
        """
        Get configuration settings from database.
        
        Args:
            category: Optional category to filter by
        
        Returns:
            Dictionary of settings by category and name, or None on error
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        try:
            # Query to get configuration settings
            if category:
                query = """
                SELECT SettingCategory, SettingName, SettingValue, Description
                FROM ConfigSetting
                WHERE SettingCategory = ?
                ORDER BY SettingCategory, SettingName
                """
                params = (category,)
            else:
                query = """
                SELECT SettingCategory, SettingName, SettingValue, Description
                FROM ConfigSetting
                ORDER BY SettingCategory, SettingName
                """
                params = None
            
            result = self.execute_query(query, params)
            
            if result:
                settings = {}
                for row in result:
                    cat, name, value, desc = row
                    if cat not in settings:
                        settings[cat] = {}
                    settings[cat][name] = {
                        'value': value,
                        'description': desc
                    }
                return settings
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting configuration settings: {str(e)}")
            return None
    
    def update_config_setting(
        self, 
        category: str, 
        name: str, 
        value: str,
        description: Optional[str] = None
    ) -> bool:
        """
        Update or insert a configuration setting.
        
        Args:
            category: Setting category
            name: Setting name
            value: Setting value
            description: Optional setting description
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            if not self.connect():
                return False
        
        try:
            # Check if setting exists
            check_query = """
            SELECT COUNT(*)
            FROM ConfigSetting
            WHERE SettingCategory = ? AND SettingName = ?
            """
            result = self.execute_query(check_query, (category, name))
            
            if result and result[0][0] > 0:
                # Update existing setting
                if description:
                    update_query = """
                    UPDATE ConfigSetting
                    SET SettingValue = ?, Description = ?
                    WHERE SettingCategory = ? AND SettingName = ?
                    """
                    self.execute_query(update_query, (value, description, category, name))
                else:
                    update_query = """
                    UPDATE ConfigSetting
                    SET SettingValue = ?
                    WHERE SettingCategory = ? AND SettingName = ?
                    """
                    self.execute_query(update_query, (value, category, name))
            else:
                # Insert new setting
                if not description:
                    description = f"{category} - {name}"
                    
                insert_query = """
                INSERT INTO ConfigSetting (SettingCategory, SettingName, SettingValue, Description)
                VALUES (?, ?, ?, ?)
                """
                self.execute_query(insert_query, (category, name, value, description))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error updating configuration setting: {str(e)}")
            return False
    
    def log_import(
        self, 
        import_type: str,
        file_name: str,
        record_count: int,
        success_count: int,
        error_count: int,
        notes: Optional[str] = None
    ) -> int:
        """
        Log an import operation.
        
        Args:
            import_type: Type of import ('PERMIT' or 'PERSONAL_PROPERTY')
            file_name: Imported file name
            record_count: Total records in file
            success_count: Successfully imported records
            error_count: Records with errors
            notes: Optional notes
        
        Returns:
            Import log ID or 0 on error
        """
        if not self.is_connected():
            if not self.connect():
                return 0
        
        try:
            # Insert import log
            query = """
            INSERT INTO ImportLog (
                ImportType, FileName, ImportDate, 
                RecordCount, SuccessCount, ErrorCount, 
                Notes
            )
            VALUES (?, ?, GETDATE(), ?, ?, ?, ?);
            
            SELECT SCOPE_IDENTITY();
            """
            
            result = self.execute_query(
                query, 
                (import_type, file_name, record_count, success_count, error_count, notes)
            )
            
            if result and len(result) > 0:
                return int(result[0][0])
            
            return 0
            
        except Exception as e:
            logger.error(f"Error logging import: {str(e)}")
            return 0
    
    def log_import_error(
        self, 
        import_log_id: int,
        record_number: int,
        error_message: str,
        record_data: Optional[str] = None
    ) -> bool:
        """
        Log an import error.
        
        Args:
            import_log_id: Import log ID
            record_number: Record number with error
            error_message: Error message
            record_data: Optional record data
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            if not self.connect():
                return False
        
        try:
            # Insert error log
            query = """
            INSERT INTO ImportErrorLog (
                ImportLogID, RecordNumber, ErrorMessage, RecordData
            )
            VALUES (?, ?, ?, ?)
            """
            
            self.execute_query(query, (import_log_id, record_number, error_message, record_data))
            self.connection.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Error logging import error: {str(e)}")
            return False
