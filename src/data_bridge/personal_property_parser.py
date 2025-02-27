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
logger = logging.getLogger(__name__)

class PersonalPropertyParser:
    """
    Parser for personal property data from various sources.
    Handles data cleaning, validation, and standardization.
    """

    def __init__(self, address_matcher: Optional[AddressMatcher] = None):
        """
        Initialize personal property parser.

        Args:
            address_matcher: Optional AddressMatcher instance for parcel lookup
        """
        self.address_matcher = address_matcher
        self.standard_fields = [
            'account_number', 'owner_name', 'owner_address', 'business_name',
            'property_location', 'property_type', 'value', 'status'
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
        Parse personal property data from file.

        Args:
            file_path: Path to data file
            format_type: Optional file format override
            sheet_name: Sheet name for Excel files
            skip_rows: Number of rows to skip
            columns_map: Mapping of source columns to standard fields

        Returns:
            DataFrame with standardized personal property data
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
            if self.address_matcher and 'property_location' in df.columns:
                df = self._match_locations(df)

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

        # Handle account numbers
        if 'account_number' in df.columns:
            # Ensure account numbers are strings
            df['account_number'] = df['account_number'].astype(str).str.strip()

            # Remove any non-alphanumeric characters except - and _
            df['account_number'] = df['account_number'].apply(
                lambda x: re.sub(r'[^\w\-]', '', x) if pd.notna(x) else x
            )

        # Clean owner names
        if 'owner_name' in df.columns:
            df['owner_name'] = df['owner_name'].astype(str).str.strip().str.title()

        # Clean owner addresses
        if 'owner_address' in df.columns:
            df['owner_address'] = df['owner_address'].astype(str).str.strip()

            # Remove extra whitespace
            df['owner_address'] = df['owner_address'].apply(
                lambda x: re.sub(r'\s+', ' ', x) if pd.notna(x) else x
            )

            # Convert to uppercase for consistency
            df['owner_address'] = df['owner_address'].str.upper()

        # Clean business names
        if 'business_name' in df.columns:
            df['business_name'] = df['business_name'].astype(str).str.strip().str.title()

        # Clean property locations
        if 'property_location' in df.columns:
            df['property_location'] = df['property_location'].astype(str).str.strip()

            # Remove extra whitespace
            df['property_location'] = df['property_location'].apply(
                lambda x: re.sub(r'\s+', ' ', x) if pd.notna(x) else x
            )

            # Convert to uppercase for consistency
            df['property_location'] = df['property_location'].str.upper()

        # Clean property types
        if 'property_type' in df.columns:
            df['property_type'] = df['property_type'].astype(str).str.strip().str.upper()

            # Standardize common property types
            type_mapping = {
                'COM': 'COMMERCIAL',
                'COMMERCIAL': 'COMMERCIAL',
                'RES': 'RESIDENTIAL',
                'RESIDENTIAL': 'RESIDENTIAL',
                'IND': 'INDUSTRIAL',
                'INDUSTRIAL': 'INDUSTRIAL',
                'AGR': 'AGRICULTURAL',
                'AGRICULTURAL': 'AGRICULTURAL',
                'MAN': 'MANUFACTURING',
                'MANUFACTURING': 'MANUFACTURING'
            }

            # Apply standardization
            for key, value in type_mapping.items():
                df.loc[df['property_type'].str.contains(key, case=False, na=False), 'property_type'] = value

        # Clean values
        if 'value' in df.columns:
            # Convert to numeric
            df['value'] = pd.to_numeric(
                df['value'].astype(str).str.replace(r'[^\d.]', '', regex=True),
                errors='coerce'
            )

            # Fill missing values with 0
            df['value'] = df['value'].fillna(0)

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

        # Validate account numbers
        if 'account_number' in df.columns:
            # Check for missing account numbers
            missing_accounts = df['account_number'].isna() | (df['account_number'] == '')
            df.loc[missing_accounts, 'validation_errors'] += 'Missing account number; '

        # Validate owner information
        if 'owner_name' in df.columns:
            # Check for missing owner names
            missing_owner = df['owner_name'].isna() | (df['owner_name'] == '')
            df.loc[missing_owner, 'validation_errors'] += 'Missing owner name; '

        # Validate property locations
        if 'property_location' in df.columns:
            # Check for missing locations
            missing_location = df['property_location'].isna() | (df['property_location'] == '')
            df.loc[missing_location, 'validation_errors'] += 'Missing property location; '

            # Check for potentially invalid locations (too short)
            short_location = df['property_location'].str.len() < 5
            df.loc[short_location & ~missing_location, 'validation_errors'] += 'Property location too short; '

        # Validate values
        if 'value' in df.columns:
            # Check for negative values
            negative_value = df['value'] < 0
            df.loc[negative_value, 'validation_errors'] += 'Negative property value; '

            # Check for very high values (potentially entered in cents)
            high_value = df['value'] > 10000000
            df.loc[high_value, 'validation_errors'] += 'Unusually high property value; '

        return df

    def _match_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Match property locations to parcels using address matcher.

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

        # Process each property location
        for idx, row in df.iterrows():
            if pd.isna(row['property_location']) or row['property_location'] == '':
                continue

            # Skip if already has a parcel ID
            if 'parcel_id' in df.columns and not pd.isna(row['parcel_id']) and row['parcel_id'] != '':
                continue

            try:
                # Match location
                matches = self.address_matcher.match_address(row['property_location'])

                if matches and len(matches) > 0:
                    # Get best match
                    best_match = matches[0]

                    # Update parcel information
                    df.at[idx, 'parcel_id'] = best_match.get('pid', '')
                    df.at[idx, 'match_confidence'] = best_match.get('confidence', 0.0)

                    # Add validation warning for low confidence matches
                    if best_match.get('confidence', 0.0) < 80.0:
                        df.at[idx, 'validation_errors'] += 'Low confidence location match; '

            except Exception as e:
                logger.error(f"Error matching location {row['property_location']}: {str(e)}")
                df.at[idx, 'validation_errors'] += 'Error matching property location; '

        return df

    def save_processed_data(self, df: pd.DataFrame, output_path: Union[str, Path]) -> bool:
        """
        Save processed personal property data to output file.

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

            logger.info(f"Saved processed personal property data to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return False