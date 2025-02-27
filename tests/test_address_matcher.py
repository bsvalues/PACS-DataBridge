
import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

# Add the src directory to the path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_bridge.address_matcher import AddressMatcher
from src.data_bridge.db_connector import DatabaseConnector

class TestAddressMatcher(unittest.TestCase):
    """Test cases for the AddressMatcher class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock database connector
        self.mock_db = MagicMock(spec=DatabaseConnector)
        
        # Sample query results
        self.sample_results = [
            ('123456', '123 MAIN ST', '123', 'MAIN', 'ST', 'ANYTOWN', 'US', '12345'),
            ('789012', '123 MAIN STREET', '123', 'MAIN', 'STREET', 'ANYTOWN', 'US', '12345'),
            ('345678', '123 N MAIN ST', '123', 'N MAIN', 'ST', 'ANYTOWN', 'US', '12345')
        ]
        
        # Configure mock to return sample results
        self.mock_db.execute_query.return_value = self.sample_results
        
        # Create address matcher with mock database
        self.matcher = AddressMatcher(self.mock_db)
    
    def test_normalize_address(self):
        """Test address normalization."""
        # Test basic normalization
        self.assertEqual(self.matcher._normalize_address('123 Main St'), '123 MAIN ST')
        
        # Test abbreviation replacement
        self.assertEqual(self.matcher._normalize_address('123 Main Street'), '123 MAIN ST')
        self.assertEqual(self.matcher._normalize_address('123 Main Avenue'), '123 MAIN AVE')
        
        # Test directional abbreviation
        self.assertEqual(self.matcher._normalize_address('123 North Main St'), '123 N MAIN ST')
        
        # Test unit/apt removal
        self.assertEqual(self.matcher._normalize_address('123 Main St, Apt 4B'), '123 MAIN ST')
        self.assertEqual(self.matcher._normalize_address('123 Main St #101'), '123 MAIN ST')
        
        # Test whitespace handling
        self.assertEqual(self.matcher._normalize_address('  123   Main   St  '), '123 MAIN ST')
        
        # Test null handling
        self.assertEqual(self.matcher._normalize_address(''), '')
        self.assertEqual(self.matcher._normalize_address(None), '')
    
    def test_parse_address(self):
        """Test address parsing."""
        # Test basic parsing
        result = self.matcher._parse_address('123 MAIN ST')
        self.assertEqual(result['street_number'], '123')
        self.assertEqual(result['street_name'], 'MAIN')
        self.assertEqual(result['street_type'], 'ST')
        
        # Test without street type
        result = self.matcher._parse_address('123 MAIN')
        self.assertEqual(result['street_number'], '123')
        self.assertEqual(result['street_name'], 'MAIN')
        self.assertEqual(result['street_type'], '')
        
        # Test directional
        result = self.matcher._parse_address('123 N MAIN ST')
        self.assertEqual(result['street_number'], '123')
        self.assertEqual(result['street_name'], 'N MAIN')
        self.assertEqual(result['street_type'], 'ST')
    
    def test_match_address(self):
        """Test address matching."""
        # Test exact match
        matches = self.matcher.match_address('123 Main St')
        self.assertTrue(len(matches) > 0)
        self.assertEqual(matches[0]['pid'], '123456')
        self.assertTrue(matches[0]['confidence'] > 90)
        
        # Test close match
        matches = self.matcher.match_address('123 Main Street')
        self.assertTrue(len(matches) > 0)
        self.assertTrue(any(m['pid'] == '789012' for m in matches))
        
        # Test with directional
        matches = self.matcher.match_address('123 North Main St')
        self.assertTrue(len(matches) > 0)
        self.assertTrue(any(m['pid'] == '345678' for m in matches))
        
        # Test no match
        self.mock_db.execute_query.return_value = []
        matches = self.matcher.match_address('999 Nonexistent Rd')
        self.assertEqual(len(matches), 0)
    
    def test_match_address_with_threshold(self):
        """Test address matching with confidence threshold."""
        # Set a high threshold
        self.matcher.set_threshold(95)
        
        # Only exact matches should pass
        matches = self.matcher.match_address('123 Main St', min_confidence=95)
        self.assertTrue(len(matches) <= 1)
        
        # Set a low threshold
        self.matcher.set_threshold(50)
        
        # More matches should pass
        matches = self.matcher.match_address('123 Main St', min_confidence=50)
        self.assertTrue(len(matches) > 0)
    
    def test_cache(self):
        """Test address cache functionality."""
        # First call should hit the database
        self.matcher.match_address('123 Main St')
        self.assertEqual(self.mock_db.execute_query.call_count, 1)
        
        # Second call should use cache
        self.matcher.match_address('123 Main St')
        self.assertEqual(self.mock_db.execute_query.call_count, 1)
        
        # Different address should hit database again
        self.matcher.match_address('456 Oak Ave')
        self.assertEqual(self.mock_db.execute_query.call_count, 2)
        
        # Clear cache and retry first address - should hit database
        self.matcher.clear_cache()
        self.matcher.match_address('123 Main St')
        self.assertEqual(self.mock_db.execute_query.call_count, 3)

if __name__ == '__main__':
    unittest.main()
