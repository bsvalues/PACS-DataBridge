
"""
Unit tests for the address_matcher module.
"""

import pytest
from data_bridge.address_matcher import AddressMatcher

class TestAddressMatcher:
    """Test cases for AddressMatcher class."""
    
    @pytest.fixture
    def matcher(self):
        """Create an AddressMatcher instance."""
        return AddressMatcher()
    
    def test_normalize_address(self, matcher):
        """Test address normalization."""
        # Test basic normalization
        assert matcher.normalize_address("123 Main St.") == "123 MAIN STREET"
        assert matcher.normalize_address("456 N. Oak Ave") == "456 NORTH OAK AVENUE"
        assert matcher.normalize_address("789 SW 1st Blvd, Apt #4") == "789 SOUTHWEST 1ST BOULEVARD APT 4"
        
        # Test handling of special characters
        assert matcher.normalize_address("123-B Main St.") == "123-B MAIN STREET"
        assert matcher.normalize_address("123 Main St. #5") == "123 MAIN STREET 5"
        
        # Test multiple spaces
        assert matcher.normalize_address("123   Main   Street") == "123 MAIN STREET"
        
        # Test empty input
        assert matcher.normalize_address("") == ""
        assert matcher.normalize_address(None) == ""
    
    def test_parse_address(self, matcher):
        """Test address parsing."""
        # Test complete address
        components = matcher.parse_address("123 N Main St, Anytown, TX 12345")
        assert components["number"] == "123"
        assert components["direction"] == "NORTH"
        assert components["street"] == "MAIN"
        assert components["street_type"] == "STREET"
        assert components["city"] == "ANYTOWN"
        assert components["state"] == "TX"
        assert components["zip"] == "12345"
        
        # Test address with unit
        components = matcher.parse_address("456 Oak Ave Apt 7B")
        assert components["number"] == "456"
        assert components["street"] == "OAK"
        assert components["street_type"] == "AVENUE"
        assert components["unit"] == "APT 7B"
        
        # Test address with only number and street
        components = matcher.parse_address("789 Broadway")
        assert components["number"] == "789"
        assert components["street"] == "BROADWAY"
    
    def test_standardize_address(self, matcher):
        """Test address standardization."""
        # Test standard address
        std_address = matcher.standardize_address("123 n main st")
        assert std_address == "123 NORTH MAIN STREET"
        
        # Test with city, state, zip
        std_address = matcher.standardize_address("456 oak ave, anytown, tx 12345")
        assert std_address == "456 OAK AVENUE, ANYTOWN, TX 12345"
        
        # Test with unit
        std_address = matcher.standardize_address("789 e broadway apt 4")
        assert std_address == "789 EAST BROADWAY APT 4"
    
    def test_match_address(self, matcher):
        """Test address matching."""
        candidates = [
            "123 Main St, Anytown, TX 12345",
            "123 Main Street, Anytown, TX 12345",
            "123 E Main St, Anytown, TX 12345",
            "125 Main St, Anytown, TX 12345",
            "123 Main St, Othertown, TX 12345"
        ]
        
        # Test exact match
        matches = matcher.match_address("123 Main St, Anytown, TX 12345", candidates)
        assert len(matches) > 0
        assert matches[0][0] == "123 Main St, Anytown, TX 12345"
        assert matches[0][1] == 100.0
        
        # Test close match
        matches = matcher.match_address("123 Main Street, Anytown, TX 12345", candidates)
        assert len(matches) > 0
        assert matches[0][0] in ["123 Main St, Anytown, TX 12345", "123 Main Street, Anytown, TX 12345"]
        assert matches[0][1] > 90.0
        
        # Test different number
        matches = matcher.match_address("124 Main St, Anytown, TX 12345", candidates)
        for match in matches:
            if match[0] == "125 Main St, Anytown, TX 12345":
                assert match[1] > 80.0  # Should still be a good match
                
        # Test filtering by minimum score
        matches = matcher.match_address("999 Different Rd, Faraway, CA 54321", candidates, min_score=90.0)
        assert len(matches) == 0  # Should have no matches above 90%
