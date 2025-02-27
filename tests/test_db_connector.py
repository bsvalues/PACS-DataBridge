"""
Unit tests for the db_connector module.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path
import pandas as pd
import pytest

# Add the src directory to the path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_bridge.db_connector import DatabaseConnector, PACSConnector, DataBridgeConnector

class TestDatabaseConnector(unittest.TestCase):
    """Test cases for the DatabaseConnector class."""

    @patch('src.data_bridge.db_connector.pyodbc')
    def setUp(self, mock_pyodbc):
        """Set up test fixtures."""
        # Configure mock cursor
        self.mock_cursor = MagicMock()
        self.mock_connection = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor

        # Configure mock pyodbc
        mock_pyodbc.connect.return_value = self.mock_connection

        # Create database connector
        self.db = DatabaseConnector(
            server='test-server',
            database='test-db',
            trusted_connection=True
        )

    def test_connect(self):
        """Test database connection."""
        # Call connect
        result = self.db.connect()

        # Check result
        self.assertTrue(result)
        self.assertEqual(self.db.connection, self.mock_connection)
        self.assertEqual(self.db.cursor, self.mock_cursor)

    def test_connect_with_credentials(self):
        """Test database connection with username/password."""
        # Create a new connector with credentials
        db = DatabaseConnector(
            server='test-server',
            database='test-db',
            username='testuser',
            password='testpass',
            trusted_connection=False
        )

        # Call connect
        result = db.connect()

        # Check result
        self.assertTrue(result)

    def test_disconnect(self):
        """Test database disconnection."""
        # Connect first
        self.db.connect()

        # Call disconnect
        self.db.disconnect()

        # Check result
        self.mock_cursor.close.assert_called_once()
        self.mock_connection.close.assert_called_once()
        self.assertIsNone(self.db.connection)
        self.assertIsNone(self.db.cursor)

    def test_is_connected(self):
        """Test connection check."""
        # Not connected initially
        self.assertFalse(self.db.is_connected())

        # Connect
        self.db.connect()

        # Should be connected
        self.mock_cursor.execute.return_value = True
        self.assertTrue(self.db.is_connected())

        # Test query failure
        self.mock_cursor.execute.side_effect = Exception("Connection lost")
        self.assertFalse(self.db.is_connected())

    def test_execute_query(self):
        """Test query execution."""
        # Configure mock for SELECT query
        self.mock_cursor.fetchall.return_value = [('value1',), ('value2',)]

        # Connect
        self.db.connect()

        # Execute query
        results = self.db.execute_query("SELECT * FROM test")

        # Check results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], 'value1')
        self.assertEqual(results[1][0], 'value2')

        # Test with parameters
        self.db.execute_query("SELECT * FROM test WHERE id = ?", (1,))
        self.mock_cursor.execute.assert_called_with("SELECT * FROM test WHERE id = ?", (1,))

    def test_insert_many(self):
        """Test bulk insert."""
        # Connect
        self.db.connect()

        # Test data
        columns = ['col1', 'col2']
        data = [('val1', 'val2'), ('val3', 'val4')]

        # Call insert_many
        result = self.db.insert_many('test_table', columns, data)

        # Check result
        self.assertTrue(result)
        self.mock_cursor.executemany.assert_called_once()
        self.mock_connection.commit.assert_called_once()

    def test_execute_stored_procedure(self):
        """Test stored procedure execution."""
        # Configure mock for stored procedure
        self.mock_cursor.fetchall.return_value = [('result1',), ('result2',)]

        # Connect
        self.db.connect()

        # Call stored procedure
        results = self.db.execute_stored_procedure('sp_test_proc')

        # Check results
        self.assertEqual(len(results), 2)

        # Test with parameters
        params = {'param1': 'value1', 'param2': 123}
        self.db.execute_stored_procedure('sp_test_proc', params)
        self.mock_cursor.execute.assert_called()

    def test_query_to_dataframe(self):
        """Test query to DataFrame conversion."""
        # Mock pd.read_sql
        with patch('src.data_bridge.db_connector.pd.read_sql') as mock_read_sql:
            # Configure mock
            mock_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            mock_read_sql.return_value = mock_df

            # Connect
            self.db.connect()

            # Execute query
            df = self.db.query_to_dataframe("SELECT * FROM test")

            # Check result
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(len(df), 2)
            mock_read_sql.assert_called_once()

    def test_table_exists(self):
        """Test table existence check."""
        # Configure mock for positive case
        self.mock_cursor.fetchall.return_value = [(1,)]

        # Connect
        self.db.connect()

        # Check existing table
        result = self.db.table_exists('existing_table')
        self.assertTrue(result)

        # Configure mock for negative case
        self.mock_cursor.fetchall.return_value = [(None,)]

        # Check non-existing table
        result = self.db.table_exists('nonexisting_table')
        self.assertFalse(result)


class TestPACSConnector:
    """Test cases for PACSConnector class."""
    
    @pytest.fixture
    def mock_db_connector(self):
        """Create a mock DatabaseConnector."""
        with patch('data_bridge.db_connector.DatabaseConnector') as mock:
            mock_instance = MagicMock()
            mock.return_value = mock_instance
            yield mock_instance
    
    @pytest.fixture
    def pacs_connector(self, mock_db_connector):
        """Create a PACSConnector instance with mocked DatabaseConnector."""
        with patch('data_bridge.db_connector.DatabaseConnector.__init__', return_value=None):
            connector = PACSConnector(
                server="test_server",
                database="PACS",
                trusted_connection=True
            )
            connector.connect = mock_db_connector.connect
            connector.execute_query = mock_db_connector.execute_query
            connector.query_to_dataframe = mock_db_connector.query_to_dataframe
            connector.is_connected = mock_db_connector.is_connected
            yield connector
    
    def test_initialization(self):
        """Test PACSConnector initialization."""
        with patch('data_bridge.db_connector.DatabaseConnector.__init__', return_value=None) as mock_init:
            connector = PACSConnector(
                server="test_server",
                database="custom_pacs_db",
                username="test_user",
                password="test_pass",
                trusted_connection=False
            )
            
            mock_init.assert_called_once_with(
                "test_server", "custom_pacs_db", "test_user", "test_pass", False, 30
            )
    
    def test_get_parcel_by_address(self, pacs_connector):
        """Test get_parcel_by_address method."""
        # Set up mock return value for execute_query
        mock_result = [(1, "123456789", "123 MAIN ST", "ANYTOWN", "TX", "12345", "JOHN DOE")]
        pacs_connector.execute_query.return_value = mock_result
        pacs_connector.is_connected.return_value = True
        
        result = pacs_connector.get_parcel_by_address("123 Main St")
        
        assert result is not None
        assert result["parcel_id"] == 1
        assert result["parcel_number"] == "123456789"
        assert result["address"] == "123 MAIN ST"
        assert result["city"] == "ANYTOWN"
        assert result["state"] == "TX"
        assert result["zip"] == "12345"
        assert result["owner_name"] == "JOHN DOE"
        
        # Test no results
        pacs_connector.execute_query.return_value = []
        result = pacs_connector.get_parcel_by_address("999 Nonexistent St")
        assert result is None
    
    def test_get_parcel_by_number(self, pacs_connector):
        """Test get_parcel_by_number method."""
        # Set up mock return value for execute_query
        mock_result = [(1, "123456789", "123 MAIN ST", "ANYTOWN", "TX", "12345", "JOHN DOE")]
        pacs_connector.execute_query.return_value = mock_result
        pacs_connector.is_connected.return_value = True
        
        result = pacs_connector.get_parcel_by_number("123456789")
        
        assert result is not None
        assert result["parcel_id"] == 1
        assert result["parcel_number"] == "123456789"
        assert result["address"] == "123 MAIN ST"
        
        # Test formatting variations
        result = pacs_connector.get_parcel_by_number("123-456-789")
        assert result is not None
    
    def test_get_permits(self, pacs_connector):
        """Test get_permits method."""
        # Mock query_to_dataframe to return a DataFrame
        mock_df = pd.DataFrame({
            "PermitID": [1, 2],
            "PermitNumber": ["BP-2023-001", "BP-2023-002"],
            "Description": ["New Construction", "Remodel"]
        })
        pacs_connector.query_to_dataframe.return_value = mock_df
        pacs_connector.is_connected.return_value = True
        
        result = pacs_connector.get_permits(limit=10, offset=0)
        
        assert result is not None
        assert len(result) == 2
        assert "PermitNumber" in result.columns
        assert result["PermitNumber"][0] == "BP-2023-001"



class TestDataBridgeConnector:
    """Test cases for DataBridgeConnector class."""
    
    @pytest.fixture
    def mock_db_connector(self):
        """Create a mock DatabaseConnector."""
        with patch('data_bridge.db_connector.DatabaseConnector') as mock:
            mock_instance = MagicMock()
            mock.return_value = mock_instance
            yield mock_instance
    
    @pytest.fixture
    def db_connector(self, mock_db_connector):
        """Create a DataBridgeConnector instance with mocked DatabaseConnector."""
        with patch('data_bridge.db_connector.DatabaseConnector.__init__', return_value=None):
            connector = DataBridgeConnector(
                server="test_server",
                database="DataBridge",
                trusted_connection=True
            )
            connector.connect = mock_db_connector.connect
            connector.execute_query = mock_db_connector.execute_query
            connector.is_connected = mock_db_connector.is_connected
            yield connector
    
    def test_initialization(self):
        """Test DataBridgeConnector initialization."""
        with patch('data_bridge.db_connector.DatabaseConnector.__init__', return_value=None) as mock_init:
            connector = DataBridgeConnector(
                server="test_server",
                database="custom_databridge_db",
                username="test_user",
                password="test_pass",
                trusted_connection=False
            )
            
            mock_init.assert_called_once_with(
                "test_server", "custom_databridge_db", "test_user", "test_pass", False, 30
            )
    
    def test_get_config_settings(self, db_connector):
        """Test get_config_settings method."""
        # Set up mock return value for execute_query
        mock_result = [
            ("Database", "PACSServer", "localhost", "PACS server name"),
            ("Database", "PACSDatabase", "PACS", "PACS database name"),
            ("Import", "PermitWatchFolder", "C:\\Import", "Permit watch folder")
        ]
        db_connector.execute_query.return_value = mock_result
        db_connector.is_connected.return_value = True
        
        result = db_connector.get_config_settings()
        
        assert result is not None
        assert "Database" in result
        assert "Import" in result
        assert "PACSServer" in result["Database"]
        assert result["Database"]["PACSServer"]["value"] == "localhost"
        assert result["Database"]["PACSServer"]["description"] == "PACS server name"
        
        # Test with category filter
        db_connector.execute_query.reset_mock()
        result = db_connector.get_config_settings(category="Database")
        
        assert db_connector.execute_query.call_args[0][1] == ("Database",)
    
    def test_update_config_setting(self, db_connector):
        """Test update_config_setting method."""
        # Mock execute_query to indicate setting exists
        db_connector.execute_query.side_effect = [[(1,)], None]
        db_connector.is_connected.return_value = True
        
        result = db_connector.update_config_setting(
            category="Database",
            name="PACSServer",
            value="new_server",
            description="Updated server name"
        )
        
        assert result == True
        assert db_connector.execute_query.call_count == 2
        assert db_connector.connection.commit.call_count == 1
        
        # Test inserting new setting
        db_connector.execute_query.reset_mock()
        db_connector.execute_query.side_effect = [[(0,)], None]
        
        result = db_connector.update_config_setting(
            category="NewCategory",
            name="NewSetting",
            value="new_value"
        )
        
        assert result == True
        assert db_connector.execute_query.call_count == 2
        
        # Check that the second call had 4 parameters (for INSERT)
        assert len(db_connector.execute_query.call_args[0][1]) == 4
    
    def test_log_import(self, db_connector):
        """Test log_import method."""
        # Mock execute_query to return an ID
        db_connector.execute_query.return_value = [(123,)]
        db_connector.is_connected.return_value = True
        
        result = db_connector.log_import(
            import_type="PERMIT",
            file_name="permits.csv",
            record_count=10,
            success_count=9,
            error_count=1,
            notes="Test import"
        )
        
        assert result == 123
        db_connector.execute_query.assert_called_once()
        
        # Test with failure
        db_connector.execute_query.return_value = None
        result = db_connector.log_import(
            import_type="PERMIT",
            file_name="permits.csv",
            record_count=10,
            success_count=9,
            error_count=1
        )
        
        assert result == 0
    
    def test_log_import_error(self, db_connector):
        """Test log_import_error method."""
        db_connector.is_connected.return_value = True
        
        result = db_connector.log_import_error(
            import_log_id=123,
            record_number=5,
            error_message="Invalid format",
            record_data="row data"
        )
        
        assert result == True
        db_connector.execute_query.assert_called_once()
        db_connector.connection.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()