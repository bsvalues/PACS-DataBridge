"""
Permit Parser Module

This module provides functionality to parse building permit data from various sources,
with a focus on CSV and Excel formats from municipalities.
"""

import os
import pandas as pd
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PermitParser:
    """Parser for building permit data from various sources."""
    
    def __init__(self, file_path: str):
        """
        Initialize the permit parser.
        
        Args:
            file_path: Path to the permit data file (CSV or Excel)
        """
        self.file_path = file_path
        self.permits = []
        self.source_name = os.path.basename(file_path)
        self.file_extension = os.path.splitext(file_path)[1].lower()
        
    def parse(self) -> List[Dict[str, Any]]:
        """
        Parse the permit data file and return structured data.
        
        Returns:
            List of dictionaries containing permit data
        """
        logger.info(f"Parsing permit data from {self.source_name}")
        
        try:
            if self.file_extension == '.csv':
                self._parse_csv()
            elif self.file_extension in ('.xlsx', '.xls'):
                self._parse_excel()
            else:
                raise ValueError(f"Unsupported file format: {self.file_extension}")
            
            logger.info(f"Successfully parsed {len(self.permits)} permits")
            return self.permits
            
        except Exception as e:
            logger.error(f"Error parsing permit data: {str(e)}")
            raise
    
    def _parse_csv(self) -> None:
        """Parse CSV format permit data."""
        # Skip header rows (typical in municipal permit reports)
        skip_rows = self._detect_header_rows()
        
        df = pd.read_csv(self.file_path, skiprows=skip_rows)
        self._process_dataframe(df)
    
    def _parse_excel(self) -> None:
        """Parse Excel format permit data."""
        # Skip header rows (typical in municipal permit reports)
        skip_rows = self._detect_header_rows(excel=True)
        
        df = pd.read_excel(self.file_path, skiprows=skip_rows)
        self._process_dataframe(df)
    
    def _detect_header_rows(self, excel: bool = False) -> int:
        """
        Detect how many header rows to skip in the file.
        
        Args:
            excel: Whether the file is Excel format
            
        Returns:
            Number of rows to skip
        """
        # This is a heuristic approach - for production, we would make this more robust
        if excel:
            df = pd.read_excel(self.file_path, nrows=10)
        else:
            df = pd.read_csv(self.file_path, nrows=10)
            
        # Look for rows that likely contain column headers (PERMIT TYPE, PERMIT NUMBER, etc.)
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
            if 'PERMIT' in row_str.upper() and 'NUMBER' in row_str.upper():
                return i
                
        # Default to skipping 3 rows (common in municipal reports)
        return 3
    
    def _process_dataframe(self, df: pd.DataFrame) -> None:
        """
        Process the dataframe and extract permit data.
        
        Args:
            df: Pandas DataFrame containing permit data
        """
        # Clean up column names - remove whitespace and standardize
        df.columns = [col.strip() for col in df.columns]
        
        # Standardize expected column names
        column_mappings = {
            'PERMIT TYPE': 'permit_type',
            'PERMIT NUMBER': 'permit_number',
            'ISSUE DATE': 'issue_date',
            'SITE ADDRESS': 'site_address',
            'DESCRIPTION': 'description',
            'OWNER NAME': 'owner_name',
            'OWNER PHONE': 'owner_phone',
            'VALUATION': 'valuation',
            'PARCEL #': 'parcel_number'
        }
        
        # Rename columns if they exist
        rename_dict = {}
        for old_name, new_name in column_mappings.items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name
        
        if rename_dict:
            df = df.rename(columns=rename_dict)
        
        # Process each permit row
        for _, row in df.iterrows():
            # Skip rows without a permit number
            if 'permit_number' in df.columns and pd.isna(row['permit_number']):
                continue
                
            # Create standardized permit dictionary
            permit = {}
            
            # Map available fields from the dataframe
            field_mappings = {
                'permit_type': 'permit_type',
                'permit_number': 'permit_number',
                'issue_date': 'issue_date',
                'site_address': 'site_address',
                'description': 'description',
                'owner_name': 'owner_name',
                'owner_phone': 'owner_phone',
                'valuation': 'valuation',
                'parcel_number': 'parcel_number'
            }
            
            for df_field, permit_field in field_mappings.items():
                if df_field in df.columns and pd.notna(row[df_field]):
                    if df_field == 'issue_date':
                        permit[permit_field] = self._parse_date(row[df_field])
                    elif df_field == 'valuation':
                        permit[permit_field] = self._parse_valuation(row[df_field])
                    elif df_field == 'parcel_number':
                        permit[permit_field] = self._clean_parcel(row[df_field])
                    else:
                        permit[permit_field] = row[df_field]
                else:
                    permit[permit_field] = None
            
            # Add the permit to our list
            self.permits.append(permit)
    
    def _parse_date(self, date_str: Union[str, datetime]) -> Optional[datetime]:
        """
        Convert date string or datetime to standard datetime format.
        
        Args:
            date_str: Date string or datetime object
            
        Returns:
            Datetime object or None if parsing fails
        """
        if isinstance(date_str, datetime):
            return date_str
            
        if pd.isna(date_str):
            return None
            
        # Try different date formats
        date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%d-%b-%Y', '%d/%m/%Y']
        
        for date_format in date_formats:
            try:
                return datetime.strptime(str(date_str).strip(), date_format)
            except ValueError:
                continue
                
        # If all attempts fail, log and return None
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def _parse_valuation(self, val_str: Any) -> float:
        """
        Extract numeric valuation from string or numeric value.
        
        Args:
            val_str: Valuation as string or number
            
        Returns:
            Valuation as float
        """
        if pd.isna(val_str):
            return 0.0
            
        # Handle numeric values
        if isinstance(val_str, (int, float)):
            return float(val_str)
            
        # Remove $ and commas, then convert to float
        val_clean = re.sub(r'[$,]', '', str(val_str))
        try:
            return float(val_clean)
        except ValueError:
            logger.warning(f"Could not parse valuation: {val_str}")
            return 0.0
    
    def _clean_parcel(self, parcel_str: Any) -> Optional[str]:
        """
        Clean up parcel number format.
        
        Args:
            parcel_str: Parcel number
            
        Returns:
            Standardized parcel number
        """
        if pd.isna(parcel_str):
            return None
            
        # Remove whitespace
        parcel = str(parcel_str).strip()
        
        # Remove any common separators and standardize format
        parcel = re.sub(r'[.\-\s]', '', parcel)
        
        return parcel
    
    def extract_improvement_details(self) -> List[Dict[str, Any]]:
        """
        Extract improvement details from permit descriptions using NLP techniques.
        
        Returns:
            List of permits with enhanced details
        """
        logger.info("Extracting improvement details from permit descriptions")
        improved_permits = []
        
        for permit in self.permits:
            if permit['description']:
                details = self._analyze_description(
                    permit['description'], 
                    permit.get('permit_type')
                )
                permit.update(details)
                
            improved_permits.append(permit)
            
        return improved_permits
    
    def _analyze_description(self, description: str, permit_type: Optional[str]) -> Dict[str, Any]:
        """
        Use basic NLP to extract details from permit description.
        
        Args:
            description: Permit description text
            permit_type: Type of permit
            
        Returns:
            Dictionary with extracted improvement details
        """
        details = {
            'improvement_type': None,
            'square_footage': None,
            'is_new_construction': False,
            'is_renovation': False,
            'is_demolition': False,
            'is_roof': False,
            'is_plumbing': False,
            'is_electrical': False,
            'is_mechanical': False
        }
        
        if not description:
            return details
            
        description = description.lower()
        
        # Detect improvement type based on permit type and description
        if permit_type:
            permit_type = permit_type.lower()
            
            if 'plumb' in permit_type:
                details['is_plumbing'] = True
            elif 'electric' in permit_type:
                details['is_electrical'] = True
            elif 'mechanical' in permit_type:
                details['is_mechanical'] = True
            elif 'roof' in permit_type:
                details['is_roof'] = True
        
        # Detect new construction
        if any(term in description for term in ['new', 'construct', 'build']):
            details['is_new_construction'] = True
            
            if any(term in description for term in ['home', 'house', 'dwelling', 'residence', 'residential']):
                details['improvement_type'] = 'New Residential Construction'
            elif any(term in description for term in ['commercial', 'office', 'retail', 'industrial']):
                details['improvement_type'] = 'New Commercial Construction'
            else:
                details['improvement_type'] = 'New Construction'
        
        # Detect renovation
        elif any(term in description for term in ['remodel', 'renovat', 'upgrad', 'improv', 'update', 'repair']):
            details['is_renovation'] = True
            details['improvement_type'] = 'Renovation'
        
        # Detect demolition
        elif any(term in description for term in ['demoli', 'remov', 'tear down', 'teardown']):
            details['is_demolition'] = True
            details['improvement_type'] = 'Demolition'
        
        # Detect roof work
        elif any(term in description for term in ['roof', 'shingle', 'reroofing']):
            details['is_roof'] = True
            details['improvement_type'] = 'Roof Work'
            
        # Detect plumbing
        elif any(term in description for term in ['plumb', 'pipe', 'water line', 'sewer']):
            details['is_plumbing'] = True
            details['improvement_type'] = 'Plumbing Work'
            
        # Detect electrical
        elif any(term in description for term in ['electric', 'wiring', 'panel']):
            details['is_electrical'] = True
            details['improvement_type'] = 'Electrical Work'
            
        # Detect mechanical
        elif any(term in description for term in ['mechanical', 'hvac', 'furnace', 'air condition']):
            details['is_mechanical'] = True
            details['improvement_type'] = 'Mechanical Work'
        
        # Extract square footage
        sf_match = re.search(r'(\d+(?:,\d+)?)\s*(?:sq\.?\s*ft\.?|square\s*feet)', description)
        if sf_match:
            sf_value = sf_match.group(1).replace(',', '')
            try:
                details['square_footage'] = float(sf_value)
            except ValueError:
                pass
        
        return details


# Example usage
if __name__ == "__main__":
    # This would be replaced with proper CLI arguments in production
    parser = PermitParser("H:/Projects/CIAPS/Permit Examples.csv")
    permits = parser.parse()
    
    print(f"Processed {len(permits)} permits")
    
    # Print first 3 permits as example
    for permit in permits[:3]:
        print(f"Permit: {permit['permit_number']}")
        print(f"Type: {permit['permit_type']}")
        print(f"Address: {permit['site_address']}")
        print(f"Parcel: {permit['parcel_number']}")
        if permit['valuation']:
            print(f"Valuation: ${permit['valuation']:,.2f}")
        print("-" * 40)
    
    # Extract improvement details
    improved_permits = parser.extract_improvement_details()
    print(f"Extracted improvement details for {len(improved_permits)} permits")
"""
Parser for building permit data from various file formats.
"""

import re
import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class PermitParser:
    """
    Parser for building permit data from various sources and formats.
    This class handles the importing, parsing, and standardization of permit data.
    """
    
    def __init__(self, field_mapping: Optional[Dict[str, str]] = None):
        """
        Initialize the permit parser.
        
        Args:
            field_mapping: Optional custom field mapping dictionary
        """
        # Default field mapping from common data sources to standardized field names
        self.field_mapping = field_mapping or {
            # Standard field names (lowercase)
            'permit_number': ['permit_number', 'permit #', 'permit', 'permit no', 'permit no.', 'permitnumber'],
            'issue_date': ['issue_date', 'issued', 'date issued', 'issuedate'],
            'description': ['description', 'desc', 'project description', 'work description'],
            'address': ['address', 'project address', 'location', 'site address', 'property address'],
            'valuation': ['valuation', 'value', 'project value', 'job value', 'construction value'],
            'permit_type': ['permit_type', 'type', 'work type', 'project type'],
            'status': ['status', 'permit status']
        }
        
        # Define improvement type keywords
        self.improvement_types = {
            'NEW_CONSTRUCTION': ['new', 'new construction', 'new home', 'new house', 'new building'],
            'ADDITION': ['addition', 'add', 'expand', 'extension'],
            'REMODEL': ['remodel', 'renovation', 'rehab', 'repair', 'update', 'upgrade'],
            'DECK': ['deck', 'patio', 'porch'],
            'ROOF': ['roof', 'roofing', 'reroof'],
            'POOL': ['pool', 'spa', 'hot tub'],
            'HVAC': ['hvac', 'air conditioning', 'furnace', 'heating'],
            'PLUMBING': ['plumbing', 'water heater', 'sewer', 'water line'],
            'ELECTRICAL': ['electrical', 'wiring', 'panel'],
            'DEMOLITION': ['demolition', 'demo', 'remove', 'tear down'],
            'TENANT_IMPROVEMENT': ['tenant improvement', 'ti ', 'tenant ', 'tenant finish']
        }
    
    def parse_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Parse permit data from a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            DataFrame with standardized permit data
        """
        file_path = Path(file_path)
        logger.info(f"Parsing permit file: {file_path}")
        
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
        
        # Standardize dates
        df = self._standardize_dates(df)
        
        # Add improvement type if missing
        if 'improvement_type' not in df.columns:
            df['improvement_type'] = self.extract_improvement_type(df)
        
        logger.info(f"Parsed {len(df)} permits")
        return df
    
    def _parse_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Parse CSV file to DataFrame.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with permit data
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
            DataFrame with permit data
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
            DataFrame with permit data
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
            df: DataFrame with permit data
            
        Returns:
            Cleaned DataFrame
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Clean valuation - remove currency symbols and commas
        if 'valuation' in df.columns:
            df['valuation'] = df['valuation'].astype(str)
            df['valuation'] = df['valuation'].str.replace('$', '', regex=False)
            df['valuation'] = df['valuation'].str.replace(',', '', regex=False)
            df['valuation'] = pd.to_numeric(df['valuation'], errors='coerce')
        
        # Clean address
        if 'address' in df.columns:
            df['address'] = df['address'].astype(str)
            # Remove special characters and extra spaces
            df['address'] = df['address'].str.replace(r'[^\w\s,.-]', '', regex=True)
            df['address'] = df['address'].str.replace(r'\s+', ' ', regex=True)
            df['address'] = df['address'].str.strip()
        
        # Fill missing values with empty strings for string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna('')
        
        return df
    
    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize date formats.
        
        Args:
            df: DataFrame with permit data
            
        Returns:
            DataFrame with standardized dates
        """
        # Standardize issue_date to ISO format (YYYY-MM-DD)
        if 'issue_date' in df.columns:
            # Convert to datetime
            df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
            
            # Convert to ISO format string
            df['issue_date'] = df['issue_date'].dt.strftime('%Y-%m-%d')
            
            # Replace NaT with empty string
            df['issue_date'] = df['issue_date'].fillna('')
        
        return df
    
    def extract_improvement_type(self, df: pd.DataFrame) -> pd.Series:
        """
        Extract improvement type from description.
        
        Args:
            df: DataFrame with permit data
            
        Returns:
            Series with improvement types
        """
        # Initialize with default
        improvement_types = pd.Series(['OTHER'] * len(df))
        
        if 'description' in df.columns and 'permit_type' in df.columns:
            # Combine description and permit_type for better detection
            text_to_analyze = df['description'].str.lower() + ' ' + df['permit_type'].str.lower()
        elif 'description' in df.columns:
            text_to_analyze = df['description'].str.lower()
        elif 'permit_type' in df.columns:
            text_to_analyze = df['permit_type'].str.lower()
        else:
            return improvement_types
        
        # Check for each improvement type
        for i, text in enumerate(text_to_analyze):
            for imp_type, keywords in self.improvement_types.items():
                if any(keyword in text for keyword in keywords):
                    improvement_types.iloc[i] = imp_type
                    break
        
        return improvement_types
    
    def extract_parcel_number(self, text: str) -> str:
        """
        Extract parcel number from text.
        
        Args:
            text: Text that may contain a parcel number
            
        Returns:
            Extracted parcel number or empty string
        """
        if not text:
            return ''
        
        # Common parcel number patterns
        patterns = [
            r'parcel\s*#?\s*(\d[\d\-]+)',
            r'apn\s*:?\s*(\d[\d\-]+)',
            r'parcel\s*id\s*:?\s*(\d[\d\-]+)',
            r'pin\s*:?\s*(\d[\d\-]+)',
            r'tax\s*id\s*:?\s*(\d[\d\-]+)'
        ]
        
        # Try each pattern
        for pattern in patterns:
            match = re.search(pattern, text.lower())
            if match:
                # Clean up the parcel number
                parcel = match.group(1)
                parcel = re.sub(r'[\-\s]', '', parcel)  # Remove hyphens and spaces
                return parcel
        
        return ''
    
    def export_to_csv(self, df: pd.DataFrame, output_path: Union[str, Path]) -> str:
        """
        Export parsed permits to CSV.
        
        Args:
            df: DataFrame with permit data
            output_path: Path to output file
            
        Returns:
            Path to exported file
        """
        output_path = Path(output_path)
        df.to_csv(output_path, index=False)
        logger.info(f"Exported {len(df)} permits to {output_path}")
        return str(output_path)
