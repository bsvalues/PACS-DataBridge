"""
Database Setup Module

Provides functionality to initialize and set up the DataBridge database,
including creating tables, indexes, and initial configuration.
"""

import os
import sys
import logging
import pyodbc
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from data_bridge.config_manager import ConfigManager
from data_bridge.db_connector import DatabaseConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseSetup:
    """
    Handles database initialization and setup for the PACS DataBridge system.
    """
    
    def __init__(self, config_manager: Optional[ConfigManager] = None):
        """
        Initialize the database setup.
        
        Args:
            config_manager: Optional ConfigManager instance
        """
        self.config = config_manager or ConfigManager()
        self.db_connector = None
        
        # Path to SQL scripts
        self.script_dir = Path(__file__).resolve().parent.parent.parent / 'docs'
        self.schema_script = self.script_dir / 'database_schema.sql'
    
    def create_database(self) -> bool:
        """
        Create the DataBridge database if it doesn't exist.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get database configuration
            db_config = self.config.get('database', 'databridge')
            if not db_config:
                logger.error("No DataBridge database configuration found")
                return False
            
            server = db_config.get('server', 'localhost')
            database = db_config.get('database', 'DataBridge')
            trusted_connection = db_config.get('trusted_connection', True)
            username = db_config.get('username', '')
            password = db_config.get('password', '')
            
            # Connect to master database to create DataBridge database
            if trusted_connection:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE=master;"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE=master;"
                    f"UID={username};"
                    f"PWD={password};"
                )
            
            # Establish connection
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT database_id FROM sys.databases WHERE name = '{database}'")
            result = cursor.fetchone()
            
            if not result:
                # Create the database
                logger.info(f"Creating database {database}")
                cursor.execute(f"CREATE DATABASE {database}")
                conn.commit()
                logger.info(f"Database {database} created successfully")
            else:
                logger.info(f"Database {database} already exists")
            
            cursor.close()
            conn.close()
            
            # Connect to the database
            self.db_connector = DatabaseConnector(
                server=server,
                database=database,
                username=username,
                password=password,
                trusted_connection=trusted_connection
            )
            
            if not self.db_connector.connect():
                logger.error(f"Failed to connect to {database} database")
                return False
            
            logger.info(f"Connected to {database} database")
            return True
            
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            return False
    
    def create_schema(self) -> bool:
        """
        Create the database schema from SQL script.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.db_connector:
                logger.error("No database connection available")
                return False
            
            if not self.schema_script.exists():
                logger.error(f"Schema script not found: {self.schema_script}")
                return False
            
            # Read the schema script
            with open(self.schema_script, 'r') as f:
                schema_sql = f.read()
            
            # Split the script into individual statements
            statements = self._split_sql_statements(schema_sql)
            
            # Execute each statement
            logger.info(f"Executing schema script with {len(statements)} statements")
            
            for i, stmt in enumerate(statements):
                if stmt.strip():
                    try:
                        self.db_connector.execute_query(stmt)
                        logger.debug(f"Executed statement {i+1}/{len(statements)}")
                    except Exception as e:
                        logger.error(f"Error executing statement {i+1}: {str(e)}")
                        logger.error(f"Statement: {stmt[:100]}...")
                        # Continue with next statement, as some might fail if objects already exist
            
            logger.info("Schema creation completed")
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            return False
    
    def _split_sql_statements(self, sql_script: str) -> List[str]:
        """
        Split a SQL script into individual statements.
        
        Args:
            sql_script: Full SQL script
            
        Returns:
            List of individual SQL statements
        """
        # Remove comments
        lines = []
        for line in sql_script.split('\n'):
            if line.strip().startswith('--'):
                continue
            lines.append(line)
        
        # Join lines and split by GO or semicolon
        sql_text = '\n'.join(lines)
        
        # Replace GO statements (SQL Server batch separator)
        statements = []
        for batch in sql_text.split('GO'):
            # Further split by semicolons (for individual statements within batches)
            for stmt in batch.split(';'):
                if stmt.strip():
                    statements.append(stmt.strip())
        
        return statements
    
    def initialize_config(self) -> bool:
        """
        Initialize configuration in the database.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.db_connector:
                logger.error("No database connection available")
                return False
            
            # Check if ConfigSetting table exists
            check_table = """
            SELECT OBJECT_ID('ConfigSetting') AS TableID
            """
            
            result = self.db_connector.execute_query(check_table)
            if not result or not result[0][0]:
                logger.warning("ConfigSetting table not found, skipping configuration initialization")
                return False
            
            # Get current settings from ConfigSetting table
            check_settings = """
            SELECT SettingCategory, SettingName, SettingValue 
            FROM ConfigSetting
            """
            
            existing_settings = self.db_connector.execute_query(check_settings)
            existing_dict = {}
            
            if existing_settings:
                for row in existing_settings:
                    category, name, value = row
                    if category not in existing_dict:
                        existing_dict[category] = {}
                    existing_dict[category][name] = value
            
            # Get settings from configuration file
            db_config = self.config.get('database', 'pacs')
            import_config = self.config.get('import')
            export_config = self.config.get('export')
            
            # Settings to add
            settings_to_add = []
            
            # Database settings
            if db_config:
                settings_to_add.extend([
                    ('Database', 'PACSServer', db_config.get('server', 'localhost')),
                    ('Database', 'PACSDatabase', db_config.get('database', 'PACS')),
                    ('Database', 'PACSAuthType', 'Windows' if db_config.get('trusted_connection', True) else 'SQL')
                ])
            
            # Import settings
            if import_config and 'permit' in import_config:
                settings_to_add.extend([
                    ('Import', 'PermitWatchFolder', import_config['permit'].get('watch_folder', '')),
                    ('Import', 'PermitArchiveFolder', import_config['permit'].get('archive_folder', ''))
                ])
            
            if import_config and 'personal_property' in import_config:
                settings_to_add.extend([
                    ('Import', 'PersonalPropertyWatchFolder', import_config['personal_property'].get('watch_folder', '')),
                    ('Import', 'PersonalPropertyArchiveFolder', import_config['personal_property'].get('archive_folder', ''))
                ])
            
            # Export settings
            if export_config:
                settings_to_add.append(
                    ('Export', 'OutputFolder', export_config.get('output_folder', ''))
                )
            
            # Add settings that don't exist yet
            for category, name, value in settings_to_add:
                if category in existing_dict and name in existing_dict[category]:
                    # Skip if setting already exists
                    continue
                
                insert_sql = """
                INSERT INTO ConfigSetting (SettingCategory, SettingName, SettingValue, Description)
                VALUES (?, ?, ?, ?)
                """
                
                description = f"{category} - {name} setting"
                self.db_connector.execute_query(insert_sql, (category, name, value, description))
            
            logger.info("Database configuration initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing configuration: {str(e)}")
            return False
    
    def setup_database(self) -> bool:
        """
        Perform complete database setup.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create database
            if not self.create_database():
                return False
            
            # Create schema
            if not self.create_schema():
                return False
            
            # Initialize configuration
            if not self.initialize_config():
                logger.warning("Failed to initialize configuration, but continuing")
            
            logger.info("Database setup completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database: {str(e)}")
            return False


def main():
    """Main entry point for database setup."""
    try:
        # Create ConfigManager
        config = ConfigManager()
        
        # Create DatabaseSetup
        db_setup = DatabaseSetup(config)
        
        # Setup database
        if db_setup.setup_database():
            print("Database setup completed successfully")
        else:
            print("Database setup failed, check the logs for details")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error in database setup: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
