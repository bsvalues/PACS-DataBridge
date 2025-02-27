"""
Command Line Interface Module

Provides a command-line interface for the PACS DataBridge system.
Enables users to perform basic operations including importing permit data,
processing personal property records, and configuring the system.
"""

import os
import sys
import argparse
import logging
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from data_bridge import __version__
from data_bridge.config_manager import ConfigManager
from data_bridge.permit_parser import PermitParser
from data_bridge.personal_property_parser import PersonalPropertyParser
from data_bridge.db_connector import DatabaseConnector, PACSConnector
from data_bridge.address_matcher import AddressMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataBridgeCLI:
    """Command-line interface for the PACS DataBridge system."""
    
    def __init__(self):
        """Initialize the CLI."""
        self.config = ConfigManager()
        self.parser = None
        self.db_connector = None
        self.address_matcher = None
    
    def setup_connections(self) -> bool:
        """
        Set up database connections based on configuration.
        
        Returns:
            True if connections were successful, False otherwise
        """
        try:
            # Get database configuration
            pacs_config = self.config.get('database', 'pacs')
            if not pacs_config:
                logger.error("No PACS database configuration found")
                return False
            
            # Connect to PACS database
            self.db_connector = PACSConnector(
                server=pacs_config.get('server', 'localhost'),
                database=pacs_config.get('database', 'PACS'),
                username=pacs_config.get('username', ''),
                password=pacs_config.get('password', ''),
                trusted_connection=pacs_config.get('trusted_connection', True)
            )
            
            if not self.db_connector.connect():
                logger.error("Failed to connect to PACS database")
                return False
            
            # Initialize address matcher with database connector
            self.address_matcher = AddressMatcher(self.db_connector)
            
            logger.info("Database connections established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up connections: {str(e)}")
            return False
    
    def import_permits(self, file_path: str, format_type: str = None, sheet_name: str = None) -> bool:
        """
        Import permit data from a file.
        
        Args:
            file_path: Path to the permit data file
            format_type: Optional file format override
            sheet_name: Sheet name for Excel files
            
        Returns:
            True if import was successful, False otherwise
        """
        try:
            logger.info(f"Importing permits from {file_path}")
            
            # Initialize permit parser
            self.parser = PermitParser(address_matcher=self.address_matcher)
            
            # Parse the file
            df = self.parser.parse_file(file_path, sheet_name=sheet_name)
            
            # Process results
            logger.info(f"Successfully parsed {len(df)} permit records")
            
            # Display statistics
            self._display_import_stats(df, 'permit')
            
            # Save to output file if needed
            output_path = f"processed_permits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_path, index=False)
            logger.info(f"Saved processed data to {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error importing permits: {str(e)}")
            return False
    
    def import_personal_property(self, file_path: str, format_type: str = None, sheet_name: str = None) -> bool:
        """
        Import personal property data from a file.
        
        Args:
            file_path: Path to the personal property data file
            format_type: Optional file format override
            sheet_name: Sheet name for Excel files
            
        Returns:
            True if import was successful, False otherwise
        """
        try:
            logger.info(f"Importing personal property data from {file_path}")
            
            # Initialize personal property parser
            pp_parser = PersonalPropertyParser(address_matcher=self.address_matcher)
            
            # Parse the file
            df = pp_parser.parse_file(file_path, sheet_name=sheet_name, skip_rows=0)
            
            # Process results
            logger.info(f"Successfully parsed {len(df)} personal property records")
            
            # Display statistics
            self._display_import_stats(df, 'personal property')
            
            # Save to output file if needed
            output_path = f"processed_property_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_path, index=False)
            logger.info(f"Saved processed data to {output_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error importing personal property: {str(e)}")
            return False
    
    def lookup_parcel(self, address: str) -> bool:
        """
        Look up a parcel by address.
        
        Args:
            address: Address to look up
            
        Returns:
            True if lookup was successful, False otherwise
        """
        try:
            if not self.db_connector:
                if not self.setup_connections():
                    logger.error("Could not set up database connection for parcel lookup")
                    return False
            
            logger.info(f"Looking up parcel for address: {address}")
            
            # Get matching parcels
            parcels = self.db_connector.get_parcel_by_address(address)
            
            if not parcels:
                logger.info("No matching parcels found")
                print("No matching parcels found")
                return True
            
            # Display results
            print(f"\nFound {len(parcels)} matching parcels:")
            print("-" * 80)
            for i, parcel in enumerate(parcels):
                print(f"{i+1}. {parcel.get('full_address', 'Unknown address')}")
                print(f"   Parcel ID: {parcel.get('pid', 'Unknown')}")
                print(f"   Location: {parcel.get('city', '')}, {parcel.get('state', '')}")
                print("-" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Error looking up parcel: {str(e)}")
            return False
    
    def setup_config(self, ciaps_config_file: str = None) -> bool:
        """
        Set up or update configuration.
        
        Args:
            ciaps_config_file: Optional path to CIAPS configuration file
            
        Returns:
            True if configuration was successful, False otherwise
        """
        try:
            if ciaps_config_file:
                logger.info(f"Importing configuration from CIAPS file: {ciaps_config_file}")
                if not self.config.setup_from_ciaps(ciaps_config_file):
                    logger.error("Failed to import CIAPS configuration")
                    return False
                logger.info("Successfully imported CIAPS configuration")
            
            # Display current configuration
            print("\nCurrent Configuration:")
            print("-" * 80)
            
            # Database configuration
            db_config = self.config.get('database', 'pacs')
            if db_config:
                print("PACS Database:")
                print(f"  Server: {db_config.get('server', 'Not set')}")
                print(f"  Database: {db_config.get('database', 'Not set')}")
                print(f"  Authentication: {'Windows' if db_config.get('trusted_connection', True) else 'SQL Server'}")
            
            # Import folder configuration
            import_config = self.config.get('import')
            if import_config:
                print("\nImport Settings:")
                if 'permit' in import_config:
                    print(f"  Permit Watch Folder: {import_config['permit'].get('watch_folder', 'Not set')}")
                    print(f"  Permit Archive Folder: {import_config['permit'].get('archive_folder', 'Not set')}")
                if 'personal_property' in import_config:
                    print(f"  Personal Property Watch Folder: {import_config['personal_property'].get('watch_folder', 'Not set')}")
                    print(f"  Personal Property Archive Folder: {import_config['personal_property'].get('archive_folder', 'Not set')}")
            
            print("-" * 80)
            print("Configuration file location:", self.config.config_file)
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting up configuration: {str(e)}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection test was successful, False otherwise
        """
        try:
            logger.info("Testing database connection")
            
            if not self.setup_connections():
                logger.error("Failed to set up database connections")
                return False
            
            # Test a simple query
            test_query = "SELECT @@VERSION AS Version"
            result = self.db_connector.execute_query(test_query)
            
            if result:
                print("\nDatabase Connection Successful!")
                print("-" * 80)
                print(f"Server Version: {result[0][0][:50]}...")
                print("-" * 80)
                logger.info("Database connection test successful")
                return True
            else:
                print("\nDatabase Connection Failed!")
                logger.error("Database connection test failed")
                return False
            
        except Exception as e:
            logger.error(f"Error testing database connection: {str(e)}")
            return False
    
    def _display_import_stats(self, df: pd.DataFrame, import_type: str) -> None:
        """
        Display statistics for an imported dataset.
        
        Args:
            df: DataFrame containing the imported data
            import_type: Type of data ('permit' or 'personal property')
        """
        print("\nImport Statistics:")
        print("-" * 80)
        print(f"Total {import_type} records: {len(df)}")
        
        # Count validation errors
        if 'validation_errors' in df.columns:
            error_count = df['validation_errors'].apply(lambda x: x != '' and not pd.isna(x)).sum()
            print(f"Records with validation errors: {error_count}")
            print(f"Valid records: {len(df) - error_count}")
        
        # Show fields mapped
        print(f"\nFields mapped:")
        for col in df.columns:
            non_null = df[col].count()
            print(f"  {col}: {non_null} values ({non_null/len(df)*100:.1f}% populated)")
        
        # Display sample data
        if len(df) > 0:
            print("\nSample record:")
            print("-" * 80)
            sample = df.iloc[0].to_dict()
            for key, value in sample.items():
                if key != 'validation_errors' and not pd.isna(value) and value != '':
                    print(f"  {key}: {value}")
        
        print("-" * 80)


def main():
    """Main entry point for the CLI."""
    # Create argument parser
    parser = argparse.ArgumentParser(
        description=f"PACS DataBridge CLI v{__version__}",
        epilog="For more information, visit https://github.com/username/PACS-DataBridge"
    )
    
    # Add subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Import permits command
    import_parser = subparsers.add_parser('import-permits', help='Import permit data')
    import_parser.add_argument('file', help='Path to permit data file (CSV or Excel)')
    import_parser.add_argument('--sheet', help='Sheet name for Excel files')
    import_parser.add_argument('--format', help='Force specific format')
    
    # Import personal property command
    pp_parser = subparsers.add_parser('import-property', help='Import personal property data')
    pp_parser.add_argument('file', help='Path to personal property data file (CSV or Excel)')
    pp_parser.add_argument('--sheet', help='Sheet name for Excel files')
    pp_parser.add_argument('--format', help='Force specific format')
    
    # Lookup parcel command
    lookup_parser = subparsers.add_parser('lookup-parcel', help='Look up parcel by address')
    lookup_parser.add_argument('address', help='Address to look up')
    
    # Configure command
    config_parser = subparsers.add_parser('config', help='Configure DataBridge')
    config_parser.add_argument('--ciaps', help='Import settings from CIAPS config file')
    
    # Test database connection command
    test_parser = subparsers.add_parser('test-connection', help='Test database connection')
    
    # Version command
    version_parser = subparsers.add_parser('version', help='Show version information')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create CLI instance
    cli = DataBridgeCLI()
    
    # Execute appropriate command
    if args.command == 'import-permits':
        cli.import_permits(args.file, args.format, args.sheet)
    elif args.command == 'import-property':
        cli.import_personal_property(args.file, args.format, args.sheet)
    elif args.command == 'lookup-parcel':
        cli.lookup_parcel(args.address)
    elif args.command == 'config':
        cli.setup_config(args.ciaps)
    elif args.command == 'test-connection':
        cli.test_connection()
    elif args.command == 'version':
        print(f"PACS DataBridge v{__version__}")
        print("A modern, AI-enhanced data import/export system for PACS TrueAutomation")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
"""
Command-line interface for PACS DataBridge.
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Optional
import logging

from data_bridge.permit_parser import PermitParser
from data_bridge.personal_property_parser import PersonalPropertyParser
from data_bridge.db_connector import PACSConnector, DataBridgeConnector
from data_bridge.config_manager import ConfigManager
from data_bridge.db_setup import DatabaseSetup
from data_bridge.address_matcher import AddressMatcher

logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Set up logging configuration.
    
    Args:
        level: Logging level
        log_file: Optional log file path
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if log file specified
    if log_file:
        try:
            # Create directory if it doesn't exist
            log_path = Path(log_file)
            os.makedirs(log_path.parent, exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(numeric_level)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to set up file logging: {str(e)}")


def import_permits(args: argparse.Namespace, config: ConfigManager) -> int:
    """
    Import permits from file.
    
    Args:
        args: Command-line arguments
        config: Configuration manager
    
    Returns:
        Exit code
    """
    try:
        # Get file path
        file_path = args.file
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return 1
        
        logger.info(f"Importing permits from {file_path}")
        
        # Parse permits
        parser = PermitParser()
        permits_df = parser.parse_file(file_path)
        
        if permits_df is None or len(permits_df) == 0:
            logger.error("No permits found in file")
            return 1
        
        logger.info(f"Found {len(permits_df)} permits")
        
        # Connect to database if requested
        if args.save_to_db:
            # Get database configuration
            db_config = config.get('database', 'databridge')
            if not db_config:
                logger.error("No database configuration found")
                return 1
            
            # Connect to database
            db = DataBridgeConnector(
                server=db_config.get('server', 'localhost'),
                database=db_config.get('database', 'DataBridge'),
                username=db_config.get('username', ''),
                password=db_config.get('password', ''),
                trusted_connection=db_config.get('trusted_connection', True)
            )
            
            if not db.connect():
                logger.error("Failed to connect to database")
                return 1
            
            # Log import
            import_log_id = db.log_import(
                import_type='PERMIT',
                file_name=os.path.basename(file_path),
                record_count=len(permits_df),
                success_count=len(permits_df),
                error_count=0,
                notes=f"Imported from {file_path}"
            )
            
            logger.info(f"Created import log record {import_log_id}")
            
            # TODO: Save permits to database
            
            db.disconnect()
        
        # Export to CSV if requested
        if args.output:
            output_path = args.output
            parser.export_to_csv(permits_df, output_path)
            logger.info(f"Exported permits to {output_path}")
        
        # Display summary
        print(f"\nImport Summary:")
        print(f"  Total records: {len(permits_df)}")
        print(f"  Successful: {len(permits_df)}")
        print(f"  Errors: 0")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error importing permits: {str(e)}")
        return 1


def import_personal_property(args: argparse.Namespace, config: ConfigManager) -> int:
    """
    Import personal property from file.
    
    Args:
        args: Command-line arguments
        config: Configuration manager
    
    Returns:
        Exit code
    """
    try:
        # Get file path
        file_path = args.file
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return 1
        
        logger.info(f"Importing personal property from {file_path}")
        
        # Parse personal property
        parser = PersonalPropertyParser()
        pp_df = parser.parse_file(file_path)
        
        if pp_df is None or len(pp_df) == 0:
            logger.error("No personal property found in file")
            return 1
        
        logger.info(f"Found {len(pp_df)} personal property accounts")
        
        # Connect to database if requested
        if args.save_to_db:
            # Get database configuration
            db_config = config.get('database', 'databridge')
            if not db_config:
                logger.error("No database configuration found")
                return 1
            
            # Connect to database
            db = DataBridgeConnector(
                server=db_config.get('server', 'localhost'),
                database=db_config.get('database', 'DataBridge'),
                username=db_config.get('username', ''),
                password=db_config.get('password', ''),
                trusted_connection=db_config.get('trusted_connection', True)
            )
            
            if not db.connect():
                logger.error("Failed to connect to database")
                return 1
            
            # Log import
            import_log_id = db.log_import(
                import_type='PERSONAL_PROPERTY',
                file_name=os.path.basename(file_path),
                record_count=len(pp_df),
                success_count=len(pp_df),
                error_count=0,
                notes=f"Imported from {file_path}"
            )
            
            logger.info(f"Created import log record {import_log_id}")
            
            # TODO: Save personal property to database
            
            db.disconnect()
        
        # Export to CSV if requested
        if args.output:
            output_path = args.output
            parser.export_to_csv(pp_df, output_path)
            logger.info(f"Exported personal property to {output_path}")
        
        # Display summary
        print(f"\nImport Summary:")
        print(f"  Total records: {len(pp_df)}")
        print(f"  Successful: {len(pp_df)}")
        print(f"  Errors: 0")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error importing personal property: {str(e)}")
        return 1


def lookup_parcel(args: argparse.Namespace, config: ConfigManager) -> int:
    """
    Look up parcel by address or parcel number.
    
    Args:
        args: Command-line arguments
        config: Configuration manager
    
    Returns:
        Exit code
    """
    try:
        # Get address or parcel number
        if args.address:
            address = args.address
            method = "address"
            search_term = address
        elif args.parcel:
            parcel_number = args.parcel
            method = "parcel number"
            search_term = parcel_number
        else:
            logger.error("No address or parcel number specified")
            return 1
        
        logger.info(f"Looking up parcel by {method}: {search_term}")
        
        # Get database configuration
        db_config = config.get('database', 'pacs')
        if not db_config:
            logger.error("No PACS database configuration found")
            return 1
        
        # Connect to database
        db = PACSConnector(
            server=db_config.get('server', 'localhost'),
            database=db_config.get('database', 'PACS'),
            username=db_config.get('username', ''),
            password=db_config.get('password', ''),
            trusted_connection=db_config.get('trusted_connection', True)
        )
        
        if not db.connect():
            logger.error("Failed to connect to database")
            return 1
        
        # Look up parcel
        if method == "address":
            # Use address matcher for address lookup
            matcher = AddressMatcher(db)
            parcels = matcher.find_parcel_by_address(address, args.min_confidence)
            
            if not parcels:
                print(f"No parcels found for address: {address}")
                return 1
            
            # Display results
            print(f"\nFound {len(parcels)} matching parcels:")
            for i, parcel in enumerate(parcels):
                print(f"\nMatch #{i+1} (Confidence: {parcel['confidence']:.1f}%):")
                print(f"  Parcel Number: {parcel['parcel_number']}")
                print(f"  Address: {parcel['address']}")
                if parcel['city'] or parcel['state'] or parcel['zip']:
                    print(f"  {parcel['city']}, {parcel['state']} {parcel['zip']}")
                print(f"  Owner: {parcel['owner_name']}")
        else:
            # Use direct lookup for parcel number
            parcel = db.get_parcel_by_number(parcel_number)
            
            if not parcel:
                print(f"No parcel found for parcel number: {parcel_number}")
                return 1
            
            # Display result
            print(f"\nFound parcel:")
            print(f"  Parcel Number: {parcel['parcel_number']}")
            print(f"  Address: {parcel['address']}")
            if parcel['city'] or parcel['state'] or parcel['zip']:
                print(f"  {parcel['city']}, {parcel['state']} {parcel['zip']}")
            print(f"  Owner: {parcel['owner_name']}")
        
        db.disconnect()
        return 0
        
    except Exception as e:
        logger.error(f"Error looking up parcel: {str(e)}")
        return 1


def setup_database(args: argparse.Namespace, config: ConfigManager) -> int:
    """
    Set up the database.
    
    Args:
        args: Command-line arguments
        config: Configuration manager
    
    Returns:
        Exit code
    """
    try:
        logger.info("Setting up DataBridge database")
        
        # Create database setup instance
        db_setup = DatabaseSetup(config)
        
        # Set up database
        success = db_setup.setup_database()
        
        if success:
            logger.info("Database setup completed successfully")
            return 0
        else:
            logger.error("Database setup failed")
            return 1
        
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        return 1


def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the CLI.
    
    Args:
        args: Optional command-line arguments
        
    Returns:
        Exit code
    """
    # Create argument parser
    parser = argparse.ArgumentParser(
        description='PACS DataBridge - County Import and Assessment Processing System'
    )
    
    # Create subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Import permits command
    import_permits_parser = subparsers.add_parser('import-permits', help='Import building permits')
    import_permits_parser.add_argument('file', help='Path to permit file')
    import_permits_parser.add_argument('-o', '--output', help='Output file path')
    import_permits_parser.add_argument('-s', '--save-to-db', action='store_true', help='Save to database')
    
    # Import personal property command
    import_pp_parser = subparsers.add_parser('import-property', help='Import personal property')
    import_pp_parser.add_argument('file', help='Path to personal property file')
    import_pp_parser.add_argument('-o', '--output', help='Output file path')
    import_pp_parser.add_argument('-s', '--save-to-db', action='store_true', help='Save to database')
    
    # Lookup parcel command
    lookup_parser = subparsers.add_parser('lookup-parcel', help='Look up parcel')
    lookup_group = lookup_parser.add_mutually_exclusive_group(required=True)
    lookup_group.add_argument('-a', '--address', help='Address to look up')
    lookup_group.add_argument('-p', '--parcel', help='Parcel number to look up')
    lookup_parser.add_argument('-c', '--min-confidence', type=float, default=70.0, help='Minimum confidence score (0-100)')
    
    # Database setup command
    db_parser = subparsers.add_parser('setup-database', help='Set up database')
    
    # Parse arguments
    parsed_args = parser.parse_args(args)
    
    # No command specified, show help
    if not parsed_args.command:
        parser.print_help()
        return 0
    
    # Load configuration
    config = ConfigManager()
    
    # Set up logging
    log_config = config.get('logging')
    if log_config:
        setup_logging(
            level=log_config.get('level', 'INFO'),
            log_file=log_config.get('log_file', None)
        )
    else:
        setup_logging()
    
    # Execute command
    if parsed_args.command == 'import-permits':
        return import_permits(parsed_args, config)
    elif parsed_args.command == 'import-property':
        return import_personal_property(parsed_args, config)
    elif parsed_args.command == 'lookup-parcel':
        return lookup_parcel(parsed_args, config)
    elif parsed_args.command == 'setup-database':
        return setup_database(parsed_args, config)
    else:
        logger.error(f"Unknown command: {parsed_args.command}")
        return 1


def cli_main() -> None:
    """Command-line entry point."""
    sys.exit(main())


if __name__ == '__main__':
    cli_main()
