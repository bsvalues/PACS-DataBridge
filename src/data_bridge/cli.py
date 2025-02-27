import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

from .config_manager import ConfigManager
from .db_connector import DatabaseConnector
from .permit_parser import PermitParser
from .personal_property_parser import PersonalPropertyParser
from .address_matcher import AddressMatcher

# Configure logging
logger = logging.getLogger(__name__)

# Version
__version__ = "1.0.0"

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
            self.db_connector = DatabaseConnector(
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
            print("Configuration file location:", self.config.config_path)

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


def cli_main():
    """Main entry point for the command-line interface."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description=f"PACS DataBridge CLI v{__version__}",
        epilog="A modern, AI-enhanced data import/export system for PACS TrueAutomation"
    )

    # Add subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Import permits command
    import_parser = subparsers.add_parser('import-permits', help='Import building permit data')
    import_parser.add_argument('file', help='Path to the permit data file')
    import_parser.add_argument('--format', help='File format (csv, xlsx, etc.)')
    import_parser.add_argument('--sheet', help='Sheet name for Excel files')

    # Import personal property command
    property_parser = subparsers.add_parser('import-property', help='Import personal property data')
    property_parser.add_argument('file', help='Path to the personal property data file')
    property_parser.add_argument('--format', help='File format (csv, xlsx, etc.)')
    property_parser.add_argument('--sheet', help='Sheet name for Excel files')

    # Lookup parcel command
    lookup_parser = subparsers.add_parser('lookup-parcel', help='Look up parcel by address')
    lookup_parser.add_argument('address', help='Address to look up')

    # Configuration command
    config_parser = subparsers.add_parser('config', help='Set up or update configuration')
    config_parser.add_argument('--ciaps', help='Path to CIAPS configuration file')

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
    cli_main()