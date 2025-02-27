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
"""
Parser for personal property data from various file formats.
"""

import os
import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class PersonalPropertyParser:
    """
    Parser for personal property data from various sources and formats.
    This class handles the importing, parsing, and standardization of personal property data.
    """
    
    def __init__(self, field_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize the personal property parser.
        
        Args:
            field_mapping: Optional custom field mapping dictionary
        """
        # Default field mapping from common data sources to standardized field names
        self.field_mapping = field_mapping or {
            # Standard field names (lowercase)
            'account_number': ['account_number', 'account #', 'account', 'acct no', 'acct #'],
            'owner_name': ['owner_name', 'owner', 'business name', 'taxpayer', 'taxpayer name'],
            'business_name': ['business_name', 'dba name', 'doing business as', 'business'],
            'address': ['address', 'location address', 'situs address', 'property address'],
            'mailing_address': ['mailing_address', 'mailing', 'taxpayer address', 'owner address'],
            'value': ['value', 'assessed value', 'total value', 'market value'],
            'property_type': ['property_type', 'type', 'category', 'classification'],
            'year': ['year', 'tax year', 'assessment year', 'fiscal year']
        }
        
        # Property type keywords for classification
        self.property_types = {
            'BUSINESS_EQUIPMENT': ['business equipment', 'equipment', 'machinery', 'computer', 'furniture'],
            'INVENTORY': ['inventory', 'merchandise', 'stock', 'supplies'],
            'LEASEHOLD_IMPROVEMENT': ['leasehold', 'improvement', 'fixture'],
            'VEHICLE': ['vehicle', 'auto', 'car', 'truck', 'fleet'],
            'AIRCRAFT': ['aircraft', 'airplane', 'aviation', 'helicopter'],
            'WATERCRAFT': ['watercraft', 'boat', 'vessel', 'ship'],
            'MOBILE_HOME': ['mobile home', 'manufactured home', 'trailer']
        }
    
    def parse_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Parse personal property data from a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            DataFrame with standardized personal property data
        """
        file_path = Path(file_path)
        logger.info(f"Parsing personal property file: {file_path}")
        
        # Check file existence
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine file type and call appropriate parser
        extension = file_path.suffix.lower()
        
        if extension == '.csv':
            df = self._parse_csv(file_path)
        elif extension in ['.xlsx', '.xls']:
            df = self._parse_excel(file_path)
        elif extension == '.json':
            df = self._parse_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {extension}")
        
        # Standardize column names
        df = self._standardize_columns(df)
        
        # Apply basic data cleaning
        df = self._clean_data(df)
        
        # Add property type if missing
        if 'property_type' not in df.columns:
            df['property_type'] = self._classify_property_type(df)
        
        logger.info(f"Parsed {len(df)} personal property accounts")
        return df
    
    def _parse_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Parse CSV file to DataFrame.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with personal property data
        """
        try:
            # Try different encodings and delimiters
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, encoding='latin1')
                except:
                    df = pd.read_csv(file_path, encoding='cp1252')
            
            # If DataFrame has only one column, try different delimiter
            if len(df.columns) == 1:
                for delimiter in [';', '|', '\t']:
                    try:
                        df = pd.read_csv(file_path, delimiter=delimiter)
                        if len(df.columns) > 1:
                            break
                    except:
                        pass
            
            return df
        except Exception as e:
            logger.error(f"Error parsing CSV file: {str(e)}")
            raise
    
    def _parse_excel(self, file_path: Path) -> pd.DataFrame:
        """
        Parse Excel file to DataFrame.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            DataFrame with personal property data
        """
        try:
            # First try with default sheet
            try:
                df = pd.read_excel(file_path)
            except:
                # Try to get sheet names and read first sheet
                xls = pd.ExcelFile(file_path)
                sheet_name = xls.sheet_names[0]
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            return df
        except Exception as e:
            logger.error(f"Error parsing Excel file: {str(e)}")
            raise
    
    def _parse_json(self, file_path: Path) -> pd.DataFrame:
        """
        Parse JSON file to DataFrame.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            DataFrame with personal property data
        """
        try:
            df = pd.read_json(file_path)
            return df
        except Exception as e:
            logger.error(f"Error parsing JSON file: {str(e)}")
            raise
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names based on field mapping.
        
        Args:
            df: DataFrame with original column names
            
        Returns:
            DataFrame with standardized column names
        """
        # Create a mapping from original column names to standardized names
        column_mapping = {}
        
        for std_name, variations in self.field_mapping.items():
            for col in df.columns:
                # Check if column name matches any variation (case insensitive)
                if col.lower() in variations or any(var in col.lower() for var in variations):
                    column_mapping[col] = std_name
                    break
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize data.
        
        Args:
            df: DataFrame with personal property data
            
        Returns:
            Cleaned DataFrame
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Clean value - remove currency symbols and commas
        if 'value' in df.columns:
            df['value'] = df['value'].astype(str)
            df['value'] = df['value'].str.replace('$', '', regex=False)
            df['value'] = df['value'].str.replace(',', '', regex=False)
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Clean addresses
        for address_col in ['address', 'mailing_address']:
            if address_col in df.columns:
                df[address_col] = df[address_col].astype(str)
                # Remove special characters and extra spaces
                df[address_col] = df[address_col].str.replace(r'[^\w\s,.-]', '', regex=True)
                df[address_col] = df[address_col].str.replace(r'\s+', ' ', regex=True)
                df[address_col] = df[address_col].str.strip()
        
        # Clean names
        for name_col in ['owner_name', 'business_name']:
            if name_col in df.columns:
                df[name_col] = df[name_col].astype(str)
                df[name_col] = df[name_col].str.strip()
        
        # Fill missing values with empty strings for string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        
        return df
    
    def _classify_property_type(self, df: pd.DataFrame) -> pd.Series:
        """
        Classify property type based on description or other fields.
        
        Args:
            df: DataFrame with personal property data
            
        Returns:
            Series with property types
        """
        # Initialize with default
        property_types = pd.Series(['UNCLASSIFIED'] * len(df))
        
        # Look for description column that might contain type info
        description_cols = [col for col in df.columns if 'description' in col.lower()]
        
        if description_cols:
            desc_col = description_cols[0]
            text_to_analyze = df[desc_col].str.lower()
        elif 'property_type' in df.columns:
            text_to_analyze = df['property_type'].str.lower()
        else:
            return property_types
        
        # Check for each property type
        for i, text in enumerate(text_to_analyze):
            if pd.isna(text) or text == '':
                continue
                
            for prop_type, keywords in self.property_types.items():
                if any(keyword in text for keyword in keywords):
                    property_types.iloc[i] = prop_type
                    break
        
        return property_types
    
    def extract_account_number(self, text: str) -> str:
        """
        Extract account number from text.
        
        Args:
            text: Text that may contain an account number
            
        Returns:
            Extracted account number or empty string
        """
        if not text:
            return ''
        
        # Common account number patterns
        patterns = [
            r'account\s*#?\s*(\w[\w\-]+)',
            r'acct\s*#?\s*(\w[\w\-]+)',
            r'id\s*:?\s*(\w[\w\-]+)'
        ]
        
        # Try each pattern
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                # Clean up the account number
                account = match.group(1)
                account = re.sub(r'[\-\s]', '', account)  # Remove hyphens and spaces
                return account
        
        return ''
    
    def export_to_csv(self, df: pd.DataFrame, output_path: Union[str, Path]) -> str:
        """
        Export parsed personal property to CSV.
        
        Args:
            df: DataFrame with personal property data
            output_path: Path to output file
            
        Returns:
            Path to exported file
        """
        output_path = Path(output_path)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} personal property accounts to {output_path}")
        return str(output_path)
