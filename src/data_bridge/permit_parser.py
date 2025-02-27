import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

from .address_matcher import AddressMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PermitParser:
    """
    Parser for building permit data from various sources.
    Handles data cleaning, validation, and standardization.
    """

    def __init__(self, address_matcher: Optional[AddressMatcher] = None):
        """
        Initialize permit parser.

        Args:
            address_matcher: Optional AddressMatcher instance for parcel lookup
        """
        self.address_matcher = address_matcher
        self.standard_fields = [
            'permit_number', 'permit_type', 'issue_date', 'address',
            'owner_name', 'valuation', 'description', 'parcel_id', 'status'
        ]

    def parse_file(
        self,
        file_path: Union[str, Path],
        format_type: Optional[str] = None,
        sheet_name: Optional[str] = None,
        skip_rows: int = 0,
        columns_map: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """
        Parse permit data from file.

        Args:
            file_path: Path to data file
            format_type: Optional file format override
            sheet_name: Sheet name for Excel files
            skip_rows: Number of rows to skip
            columns_map: Mapping of source columns to standard fields

        Returns:
            DataFrame with standardized permit data
        """
        file_path = Path(file_path) if isinstance(file_path, str) else file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine file format if not specified
        if format_type is None:
            format_type = file_path.suffix.lower().lstrip('.')

        try:
            # Parse file based on format
            if format_type in ['csv', 'txt']:
                df = pd.read_csv(file_path, skiprows=skip_rows)
            elif format_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows)
            else:
                raise ValueError(f"Unsupported file format: {format_type}")

            logger.info(f"Successfully read {len(df)} records from {file_path}")

            # Apply column mapping if provided
            if columns_map:
                df = self._apply_column_mapping(df, columns_map)

            # Clean and standardize data
            df = self._clean_data(df)

            # Validate data
            df = self._validate_data(df)

            # Match addresses to parcels if address matcher is available
            if self.address_matcher and 'address' in df.columns:
                df = self._match_addresses(df)

            return df

        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {str(e)}")
            raise

    def _apply_column_mapping(self, df: pd.DataFrame, columns_map: Dict[str, str]) -> pd.DataFrame:
        """
        Apply column mapping to rename columns to standard fields.

        Args:
            df: Source DataFrame
            columns_map: Mapping of source columns to standard fields

        Returns:
            DataFrame with renamed columns
        """
        # Create a new DataFrame with standardized columns
        new_df = pd.DataFrame()

        # Apply mapping
        for target_col, source_col in columns_map.items():
            if source_col in df.columns:
                new_df[target_col] = df[source_col]

        # Add validation column
        new_df['validation_errors'] = ''

        return new_df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize data.

        Args:
            df: Source DataFrame

        Returns:
            Cleaned DataFrame
        """
        # Make a copy to avoid modifying the original
        df = df.copy()

        # Clean up column names
        df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

        # Handle permit numbers
        if 'permit_number' in df.columns:
            # Ensure permit numbers are strings
            df['permit_number'] = df['permit_number'].astype(str).str.strip()

            # Remove any non-alphanumeric characters except - and _
            df['permit_number'] = df['permit_number'].apply(
                lambda x: re.sub(r'[^\w\-]', '', x) if pd.notna(x) else x
            )

        # Handle dates
        if 'issue_date' in df.columns:
            # Convert to datetime
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')

            # Fill missing dates with current date
            df['issue_date'] = df['issue_date'].fillna(pd.Timestamp.now().date())

        # Clean addresses
        if 'address' in df.columns:
            df['address'] = df['address'].astype(str).str.strip()

            # Remove extra whitespace
            df['address'] = df['address'].apply(
                lambda x: re.sub(r'\s+', ' ', x) if pd.notna(x) else x
            )

            # Convert to uppercase for consistency
            df['address'] = df['address'].str.upper()

        # Clean permit types
        if 'permit_type' in df.columns:
            df['permit_type'] = df['permit_type'].astype(str).str.strip().str.upper()

            # Standardize common permit types
            type_mapping = {
                'NEW': 'NEW CONSTRUCTION',
                'ADDITION': 'ADDITION',
                'REMODEL': 'REMODEL',
                'ALT': 'ALTERATION',
                'ALTERATION': 'ALTERATION',
                'REPAIR': 'REPAIR',
                'DEMO': 'DEMOLITION',
                'DEMOLITION': 'DEMOLITION',
                'ELEC': 'ELECTRICAL',
                'ELECTRICAL': 'ELECTRICAL',
                'PLUMB': 'PLUMBING',
                'PLUMBING': 'PLUMBING',
                'MECH': 'MECHANICAL',
                'MECHANICAL': 'MECHANICAL'
            }

            # Apply standardization
            for key, value in type_mapping.items():
                df.loc[df['permit_type'].str.contains(key, case=False, na=False), 'permit_type'] = value

        # Clean valuation
        if 'valuation' in df.columns:
            # Convert to numeric
            df['valuation'] = pd.to_numeric(
                df['valuation'].astype(str).str.replace(r'[^\d.]', '', regex=True),
                errors='coerce'
            )

            # Fill missing valuations with 0
            df['valuation'] = df['valuation'].fillna(0)

        # Clean owner names
        if 'owner_name' in df.columns:
            df['owner_name'] = df['owner_name'].astype(str).str.strip().str.title()

        # Clean descriptions
        if 'description' in df.columns:
            df['description'] = df['description'].astype(str).str.strip()

            # Remove extra whitespace
            df['description'] = df['description'].apply(
                lambda x: re.sub(r'\s+', ' ', x) if pd.notna(x) else x
            )

        # Clean parcel IDs
        if 'parcel_id' in df.columns:
            df['parcel_id'] = df['parcel_id'].astype(str).str.strip()

            # Remove any non-alphanumeric characters except - and _
            df['parcel_id'] = df['parcel_id'].apply(
                lambda x: re.sub(r'[^\w\-]', '', x) if pd.notna(x) else x
            )

        # Clean status
        if 'status' in df.columns:
            df['status'] = df['status'].astype(str).str.strip().str.upper()

        return df

    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate data and add validation errors.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with validation errors
        """
        # Initialize validation errors column if not present
        if 'validation_errors' not in df.columns:
            df['validation_errors'] = ''

        # Validate permit numbers
        if 'permit_number' in df.columns:
            # Check for missing permit numbers
            missing_permits = df['permit_number'].isna() | (df['permit_number'] == '')
            df.loc[missing_permits, 'validation_errors'] += 'Missing permit number; '

        # Validate addresses
        if 'address' in df.columns:
            # Check for missing addresses
            missing_address = df['address'].isna() | (df['address'] == '')
            df.loc[missing_address, 'validation_errors'] += 'Missing address; '

            # Check for potentially invalid addresses (too short)
            short_address = df['address'].str.len() < 5
            df.loc[short_address & ~missing_address, 'validation_errors'] += 'Address too short; '

        # Validate valuation
        if 'valuation' in df.columns:
            # Check for negative valuation
            negative_valuation = df['valuation'] < 0
            df.loc[negative_valuation, 'validation_errors'] += 'Negative valuation; '

        # Validate issue dates
        if 'issue_date' in df.columns:
            # Check for future dates
            future_dates = df['issue_date'] > pd.Timestamp.now()
            df.loc[future_dates, 'validation_errors'] += 'Future issue date; '

            # Check for very old dates (more than 50 years ago)
            old_dates = df['issue_date'] < (pd.Timestamp.now() - pd.DateOffset(years=50))
            df.loc[old_dates, 'validation_errors'] += 'Very old issue date; '

        return df

    def _match_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Match addresses to parcels using address matcher.

        Args:
            df: Source DataFrame

        Returns:
            DataFrame with matched parcel information
        """
        # Ensure address matcher is available
        if not self.address_matcher:
            logger.warning("Address matcher not available for parcel matching")
            return df

        # Add columns for match results
        if 'parcel_id' not in df.columns:
            df['parcel_id'] = ''

        if 'match_confidence' not in df.columns:
            df['match_confidence'] = 0.0

        # Process each address
        for idx, row in df.iterrows():
            if pd.isna(row['address']) or row['address'] == '':
                continue

            # Skip if already has a parcel ID
            if 'parcel_id' in df.columns and not pd.isna(row['parcel_id']) and row['parcel_id'] != '':
                continue

            try:
                # Match address
                matches = self.address_matcher.match_address(row['address'])

                if matches and len(matches) > 0:
                    # Get best match
                    best_match = matches[0]

                    # Update parcel information
                    df.at[idx, 'parcel_id'] = best_match.get('pid', '')
                    df.at[idx, 'match_confidence'] = best_match.get('confidence', 0.0)

                    # Add validation warning for low confidence matches
                    if best_match.get('confidence', 0.0) < 80.0:
                        df.at[idx, 'validation_errors'] += 'Low confidence address match; '

            except Exception as e:
                logger.error(f"Error matching address {row['address']}: {str(e)}")
                df.at[idx, 'validation_errors'] += 'Error matching address; '

        return df

    def save_processed_data(self, df: pd.DataFrame, output_path: Union[str, Path]) -> bool:
        """
        Save processed permit data to output file.

        Args:
            df: DataFrame with processed data
            output_path: Path to output file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert output_path to Path object
            output_path = Path(output_path) if isinstance(output_path, str) else output_path

            # Create directory if it doesn't exist
            os.makedirs(output_path.parent, exist_ok=True)

            # Determine file format based on extension
            format_type = output_path.suffix.lower().lstrip('.')

            # Save file in appropriate format
            if format_type == 'csv':
                df.to_csv(output_path, index=False)
            elif format_type in ['xlsx', 'xls']:
                df.to_excel(output_path, index=False)
            else:
                logger.warning(f"Unsupported output format: {format_type}, defaulting to CSV")
                output_path = output_path.with_suffix('.csv')
                df.to_csv(output_path, index=False)

            logger.info(f"Saved processed permit data to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return False

# Example usage (replace with actual file path)
if __name__ == "__main__":
    #This is just for testing purposes. Replace with your actual file path and arguments
    file_path = "H:/Projects/CIAPS/Permit Examples.csv"
    parser = PermitParser()
    try:
        permits_df = parser.parse_file(file_path)
        print(f"Processed {len(permits_df)} permits")
        print(permits_df.head())

    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")