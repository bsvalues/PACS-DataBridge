"""
Personal Property Parser Module

Provides functionality to parse and process personal property data files
for import into the PACS TrueAutomation system.
"""

import os
import re
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Tuple
from pathlib import Path

from data_bridge.address_matcher import AddressMatcher, Address

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PersonalPropertyParser:
    """
    Parser for personal property declaration data from various source formats.
    Supports standardization, validation, and preparation for PACS import.
    """
    
    # Standard field mappings for personal property data
    DEFAULT_FIELD_MAPPING = {
        'taxpayer_id': ['taxpayer_id', 'taxpayer id', 'account_number', 'account number', 'id'],
        'business_name': ['business_name', 'business name', 'company_name', 'company name', 'name'],
        'taxpayer_name': ['taxpayer_name', 'taxpayer name', 'owner_name', 'owner name', 'responsible_party'],
        'address': ['address', 'location', 'property_address', 'property address', 'site_address'],
        'mailing_address': ['mailing_address', 'mailing address', 'mail_address', 'mail address'],
        'city': ['city', 'site_city', 'property_city'],
        'state': ['state', 'site_state', 'property_state'],
        'zip': ['zip', 'zipcode', 'zip_code', 'postal_code', 'site_zip'],
        'parcel_number': ['parcel_number', 'parcel number', 'parcel_id', 'parcel id', 'pid', 'apn'],
        'property_type': ['property_type', 'property type', 'asset_type', 'asset type', 'type'],
        'description': ['description', 'asset_description', 'property_description'],
        'acquisition_date': ['acquisition_date', 'acquisition date', 'date_acquired', 'purchase_date'],
        'acquisition_cost': ['acquisition_cost', 'acquisition cost', 'original_cost', 'purchase_cost', 'cost'],
        'quantity': ['quantity', 'qty', 'asset_count', 'count', 'units'],
        'year': ['year', 'model_year', 'asset_year', 'manufacture_year'],
        'make': ['make', 'manufacturer', 'brand'],
        'model': ['model', 'model_number', 'model_name'],
        'serial_number': ['serial_number', 'serial number', 'serial_no', 'serial'],
        'condition': ['condition', 'asset_condition', 'status'],
        'category': ['category', 'asset_category', 'class', 'classification']
    }
    
    def __init__(
        self, 
        field_mapping: Optional[Dict[str, List[str]]] = None,
        address_matcher: Optional[AddressMatcher] = None
    ):
        """
        Initialize the PersonalPropertyParser.
        
        Args:
            field_mapping: Custom field mapping dictionary (keys are standard fields, 
                          values are lists of potential source field names)
            address_matcher: Optional AddressMatcher for address standardization
        """
        self.field_mapping = field_mapping or self.DEFAULT_FIELD_MAPPING
        self.address_matcher = address_matcher
    
    def parse_file(
        self, 
        file_path: str,
        sheet_name: Optional[str] = None,
        skip_rows: int = 0,
        custom_mapping: Optional[Dict[str, str]] = None
    ) -> pd.DataFrame:
        """
        Parse a personal property data file.
        
        Args:
            file_path: Path to the file to parse
            sheet_name: Sheet name for Excel files (None for first sheet)
            skip_rows: Number of header rows to skip
            custom_mapping: Override for standard field mapping
            
        Returns:
            DataFrame with standardized personal property data
        """
        file_path = Path(file_path)
        
        # Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            # Determine file type and read accordingly
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows)
            elif file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, skiprows=skip_rows)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
            
            # Clean column names
            df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
            
            # Apply field mapping
            mapping = custom_mapping or self._detect_field_mapping(df)
            if mapping:
                df_mapped = self._apply_field_mapping(df, mapping)
            else:
                df_mapped = df.copy()
                logger.warning("Could not detect field mapping, using original columns")
            
            # Perform data standardization and cleaning
            df_cleaned = self._standardize_data(df_mapped)
            
            # Validate the data
            validation_results = self._validate_data(df_cleaned)
            
            # Add validation results to the DataFrame
            df_cleaned['validation_errors'] = validation_results
            
            return df_cleaned
            
        except Exception as e:
            logger.error(f"Error parsing file {file_path}: {str(e)}")
            raise
    
    def _detect_field_mapping(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Automatically detect field mapping from DataFrame columns.
        
        Args:
            df: DataFrame with original columns
            
        Returns:
            Dictionary mapping standard fields to actual column names
        """
        mapping = {}
        
        # Iterate through our standard fields
        for std_field, possible_names in self.field_mapping.items():
            # Check for exact matches first
            exact_matches = [col for col in df.columns if col in possible_names]
            
            if exact_matches:
                mapping[std_field] = exact_matches[0]
                continue
            
            # Check for partial matches
            partial_matches = []
            for col in df.columns:
                for name in possible_names:
                    if name in col or col in name:
                        partial_matches.append((col, len(name) / len(col) if len(col) > 0 else 0))
            
            # Sort by match quality (higher is better)
            if partial_matches:
                partial_matches.sort(key=lambda x: x[1], reverse=True)
                mapping[std_field] = partial_matches[0][0]
        
        return mapping
    
    def _apply_field_mapping(
        self, 
        df: pd.DataFrame, 
        mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Apply field mapping to standardize column names.
        
        Args:
            df: Original DataFrame
            mapping: Dictionary mapping standard fields to actual column names
            
        Returns:
            DataFrame with standardized column names
        """
        # Create a new DataFrame with our standard fields
        result = pd.DataFrame()
        
        # Copy data from original columns to standard columns
        for std_field, orig_field in mapping.items():
            if orig_field in df.columns:
                result[std_field] = df[orig_field]
            else:
                # Field not found in data
                result[std_field] = np.nan
        
        # Also copy any unmatched columns
        unmatched_cols = set(df.columns) - set(mapping.values())
        for col in unmatched_cols:
            result[col] = df[col]
        
        return result
    
    def _standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the data.
        
        Args:
            df: DataFrame with data to standardize
            
        Returns:
            DataFrame with standardized data
        """
        result = df.copy()
        
        # Fill missing values
        result = result.fillna('')
        
        # Standardize dates
        date_columns = ['acquisition_date']
        for col in date_columns:
            if col in result.columns:
                result[col] = self._standardize_dates(result[col])
        
        # Standardize numeric values
        numeric_columns = ['acquisition_cost', 'quantity']
        for col in numeric_columns:
            if col in result.columns:
                result[col] = self._standardize_numeric(result[col])
        
        # Standardize addresses
        address_columns = ['address', 'mailing_address']
        for col in address_columns:
            if col in result.columns and self.address_matcher:
                result[f'standardized_{col}'] = result[col].apply(
                    lambda x: self.address_matcher.standardize_address(str(x)).full_address 
                    if x else ''
                )
        
        # Standardize property types
        if 'property_type' in result.columns:
            result['property_type'] = self._standardize_property_type(result['property_type'])
        
        # Convert empty strings back to NaN for consistency
        result = result.replace('', np.nan)
        
        return result
    
    def _standardize_dates(self, date_series: pd.Series) -> pd.Series:
        """
        Standardize date values to ISO format.
        
        Args:
            date_series: Series containing date values
            
        Returns:
            Series with standardized dates
        """
        # Define a function to parse individual date values
        def parse_date(date_val):
            if pd.isna(date_val) or date_val == '':
                return np.nan
                
            if isinstance(date_val, (datetime, pd.Timestamp)):
                return date_val.strftime('%Y-%m-%d')
                
            # Try different date formats
            date_formats = [
                '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d',
                '%m/%d/%y', '%d/%m/%y', '%m-%d-%y', '%y/%m/%d',
                '%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y'
            ]
            
            # Convert to string
            date_str = str(date_val).strip()
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Handle Excel date numbers
            try:
                float_val = float(date_str)
                # Excel dates are number of days since 1900-01-01 (with some quirks)
                parsed_date = datetime(1899, 12, 30) + pd.Timedelta(days=float_val)
                return parsed_date.strftime('%Y-%m-%d')
            except (ValueError, OverflowError):
                pass
            
            # Could not parse the date
            logger.warning(f"Could not parse date value: {date_val}")
            return np.nan
        
        return date_series.apply(parse_date)
    
    def _standardize_numeric(self, numeric_series: pd.Series) -> pd.Series:
        """
        Standardize numeric values.
        
        Args:
            numeric_series: Series containing numeric values
            
        Returns:
            Series with standardized numeric values
        """
        def parse_numeric(value):
            if pd.isna(value) or value == '':
                return np.nan
                
            if isinstance(value, (int, float)):
                return float(value)
                
            # Convert to string and clean
            value_str = str(value).strip()
            
            # Remove currency symbols and other non-numeric characters
            value_str = re.sub(r'[^\d.\-,]', '', value_str)
            
            # Handle commas (could be thousands separators or decimal points)
            if ',' in value_str and '.' in value_str:
                # Both comma and period - assume comma is thousands separator
                value_str = value_str.replace(',', '')
            elif ',' in value_str:
                # Only commas - could be decimal points in some locales
                if value_str.count(',') == 1 and value_str.rfind(',') > value_str.rfind('.'):
                    # Likely a decimal point
                    value_str = value_str.replace(',', '.')
                else:
                    # Likely thousands separators
                    value_str = value_str.replace(',', '')
            
            try:
                return float(value_str)
            except ValueError:
                logger.warning(f"Could not parse numeric value: {value}")
                return np.nan
        
        return numeric_series.apply(parse_numeric)
    
    def _standardize_property_type(self, type_series: pd.Series) -> pd.Series:
        """
        Standardize property type values.
        
        Args:
            type_series: Series containing property type values
            
        Returns:
            Series with standardized property types
        """
        # Define common property type mappings
        type_mappings = {
            'computer': 'COMPUTER_EQUIPMENT',
            'computers': 'COMPUTER_EQUIPMENT',
            'computer equipment': 'COMPUTER_EQUIPMENT',
            'it equipment': 'COMPUTER_EQUIPMENT',
            'computer hardware': 'COMPUTER_EQUIPMENT',
            
            'furniture': 'FURNITURE',
            'office furniture': 'FURNITURE',
            'fixtures': 'FURNITURE',
            'furniture and fixtures': 'FURNITURE',
            
            'machinery': 'MACHINERY_EQUIPMENT',
            'equipment': 'MACHINERY_EQUIPMENT',
            'machinery & equipment': 'MACHINERY_EQUIPMENT',
            'machinery and equipment': 'MACHINERY_EQUIPMENT',
            'manufacturing equipment': 'MACHINERY_EQUIPMENT',
            
            'vehicle': 'VEHICLE',
            'vehicles': 'VEHICLE',
            'auto': 'VEHICLE',
            'automobile': 'VEHICLE',
            'truck': 'VEHICLE',
            'car': 'VEHICLE',
            
            'inventory': 'INVENTORY',
            'stock': 'INVENTORY',
            'supplies': 'SUPPLIES',
            'leasehold': 'LEASEHOLD_IMPROVEMENT',
            'leasehold improvement': 'LEASEHOLD_IMPROVEMENT',
            'improvement': 'LEASEHOLD_IMPROVEMENT',
            
            'intangible': 'INTANGIBLE',
            'goodwill': 'INTANGIBLE',
            'intellectual property': 'INTANGIBLE',
            
            'other': 'OTHER'
        }
        
        def standardize_type(type_val):
            if pd.isna(type_val) or type_val == '':
                return 'UNKNOWN'
                
            type_str = str(type_val).strip().lower()
            
            # Try direct mapping
            if type_str in type_mappings:
                return type_mappings[type_str]
                
            # Try partial matching
            for key, value in type_mappings.items():
                if key in type_str or type_str in key:
                    return value
            
            # Default
            return 'OTHER'
        
        return type_series.apply(standardize_type)
    
    def _validate_data(self, df: pd.DataFrame) -> pd.Series:
        """
        Validate the data and return validation errors.
        
        Args:
            df: DataFrame with data to validate
            
        Returns:
            Series with validation error messages
        """
        validation_errors = []
        
        # Check each row
        for idx, row in df.iterrows():
            row_errors = []
            
            # Check required fields
            required_fields = ['business_name', 'taxpayer_name', 'address']
            for field in required_fields:
                if field in row and (pd.isna(row[field]) or row[field] == ''):
                    row_errors.append(f"Missing required field: {field}")
            
            # Validate acquisition cost
            if 'acquisition_cost' in row and not pd.isna(row['acquisition_cost']):
                try:
                    cost = float(row['acquisition_cost'])
                    if cost < 0:
                        row_errors.append("Acquisition cost cannot be negative")
                except (ValueError, TypeError):
                    row_errors.append("Invalid acquisition cost")
            
            # Validate acquisition date
            if 'acquisition_date' in row and not pd.isna(row['acquisition_date']):
                try:
                    # Assuming standardized ISO format date
                    date = datetime.strptime(row['acquisition_date'], '%Y-%m-%d')
                    # Check if date is in the future
                    if date > datetime.now():
                        row_errors.append("Acquisition date cannot be in the future")
                except (ValueError, TypeError):
                    row_errors.append("Invalid acquisition date format")
            
            validation_errors.append('; '.join(row_errors) if row_errors else '')
        
        return pd.Series(validation_errors)


# Example usage
if __name__ == "__main__":
    # Create a personal property parser
    parser = PersonalPropertyParser()
    
    # Example file path (would be replaced with actual file in production)
    # file_path = "path/to/personal_property.xlsx"
    
    # For demo purposes, show how it would be used
    print("PersonalPropertyParser usage example:")
    print("parser = PersonalPropertyParser()")
    print("property_data = parser.parse_file('path/to/personal_property.xlsx')")
    print("print(f'Parsed {len(property_data)} personal property records')")
    print("print(property_data.head())")
    print("")
    print("# Check validation errors")
    print("errors = property_data[property_data['validation_errors'] != '']")
    print("print(f'Found {len(errors)} records with validation errors')")
    print("print(errors[['business_name', 'validation_errors']])")
