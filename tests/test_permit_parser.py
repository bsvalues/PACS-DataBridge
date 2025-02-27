"""
Unit tests for the permit_parser module.
"""

import os
import pytest
import pandas as pd
from pathlib import Path

from data_bridge.permit_parser import PermitParser

# Sample test data
SAMPLE_PERMIT_DATA = """
Permit Number,Issue Date,Description,Address,Valuation,Type,Status
BP-2023-001,01/15/2023,New Single Family Home,123 Main St,350000,Residential,Active
BP-2023-002,01/20/2023,Kitchen Remodel,456 Oak Ave,45000,Residential,Active
BP-2023-003,01/25/2023,Commercial Tenant Improvement,789 Business Park,125000,Commercial,Active
"""

class TestPermitParser:
    """Test cases for PermitParser class."""
    
    @pytest.fixture
    def sample_csv_path(self, tmp_path):
        """Create a sample CSV file for testing."""
        file_path = tmp_path / "sample_permits.csv"
        with open(file_path, "w") as f:
            f.write(SAMPLE_PERMIT_DATA)
        return file_path
    
    @pytest.fixture
    def parser(self):
        """Create a PermitParser instance."""
        return PermitParser()
    
    def test_parser_initialization(self, parser):
        """Test that parser initializes correctly."""
        assert parser is not None
        assert parser.field_mapping is not None
    
    def test_parse_file_csv(self, parser, sample_csv_path):
        """Test parsing a CSV file."""
        df = parser.parse_file(sample_csv_path)
        
        # Check that we got the expected number of rows
        assert len(df) == 3
        
        # Check that fields were correctly mapped
        assert 'permit_number' in df.columns
        assert 'description' in df.columns
        assert 'valuation' in df.columns
        
        # Check that data was correctly parsed
        assert df['permit_number'].iloc[0] == 'BP-2023-001'
        assert df['address'].iloc[1] == '456 Oak Ave'
        assert df['valuation'].iloc[2] == 125000
    
    def test_parse_dates(self, parser, sample_csv_path):
        """Test date parsing functionality."""
        df = parser.parse_file(sample_csv_path)
        
        # Check that dates were standardized to ISO format
        assert df['issue_date'].iloc[0] == '2023-01-15'
        assert df['issue_date'].iloc[1] == '2023-01-20'
        assert df['issue_date'].iloc[2] == '2023-01-25'
    
    def test_permit_type_detection(self, parser, sample_csv_path):
        """Test permit type detection."""
        df = parser.parse_file(sample_csv_path)
        
        # Check that permit types were correctly determined
        improvements = parser.extract_improvement_type(df)
        
        assert improvements.iloc[0] == 'NEW_CONSTRUCTION'  # New Single Family Home
        assert improvements.iloc[1] == 'REMODEL'  # Kitchen Remodel
        assert improvements.iloc[2] == 'TENANT_IMPROVEMENT'  # Commercial Tenant Improvement
    
    def test_extract_parcel_numbers(self, parser):
        """Test parcel number extraction from text."""
        # Sample text with parcel numbers in different formats
        text1 = "Property located at 123 Main St, Parcel #12345678"
        text2 = "APN: 123-456-789 for the property at 456 Oak Ave"
        text3 = "Building permit for 789 Business Park (Parcel ID: 98765432)"
        
        # Test extraction
        assert parser.extract_parcel_number(text1) == '12345678'
        assert parser.extract_parcel_number(text2) == '123456789'
        assert parser.extract_parcel_number(text3) == '98765432'
    
    # More tests would be added for other functionality...
