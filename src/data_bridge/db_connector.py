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
