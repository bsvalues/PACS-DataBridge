
"""
Unit tests for the db_connector module.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from data_bridge.db_connector import DatabaseConnector, PACSConnector, DataBridgeConnector

class TestDatabaseConnector:
    """Test cases for DatabaseConnector class."""
    
    @pytest.fixture
    def mock_pyodbc(self):
        """Create a mock pyodbc module."""
        with patch('data_bridge.db_connector.pyodbc') as mock:
            # Mock the cursor and connection
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock.connect.return_value = mock_connection
            
            yield mock
    
    @pytest.fixture
    def connector(self, mock_pyodbc):
        """Create a DatabaseConnector instance with mocked pyodbc."""
        return DatabaseConnector(
            server="test_server",
            database="test_db",
            username="test_user",
            password="test_pass",
            trusted_connection=False
        )
    
    def test_initialization(self, connector):
        """Test connector initialization."""
        assert connector.server == "test_server"
        assert connector.database == "test_db"
        assert connector.username == "test_user"
        assert connector.password == "test_pass"
        assert connector.trusted_connection == False
        assert connector.connection is None
        assert connector.cursor is None
    
    def test_connect_trusted(self, mock_pyodbc):
        """Test connection with trusted authentication."""
        connector = DatabaseConnector(
            server="test_server",
            database="test_db",
            trusted_connection=True
        )
        
        result = connector.connect()
        
        assert result == True
        assert connector.connection is not None
        assert connector.cursor is not None
        
        # Check that pyodbc.connect was called with correct connection string
        mock_pyodbc.connect.assert_called_once()
        conn_str = mock_pyodbc.connect.call_args[0][0]
        assert "SERVER=test_server" in conn_str
        assert "DATABASE=test_db" in conn_str
        assert "Trusted_Connection=yes" in conn_str
    
    def test_connect_sql_auth(self, mock_pyodbc):
        """Test connection with SQL authentication."""
        connector = DatabaseConnector(
            server="test_server",
            database="test_db",
            username="test_user",
            password="test_pass",
            trusted_connection=False
        )
        
        result = connector.connect()
        
        assert result == True
        assert connector.connection is not None
        assert connector.cursor is not None
        
        # Check that pyodbc.connect was called with correct connection string
        mock_pyodbc.connect.assert_called_once()
        conn_str = mock_pyodbc.connect.call_args[0][0]
        assert "SERVER=test_server" in conn_str
        assert "DATABASE=test_db" in conn_str
        assert "UID=test_user" in conn_str
        assert "PWD=test_pass" in conn_str
    
    def test_disconnect(self, connector):
        """Test disconnection."""
        connector.connect()
        
        connector.disconnect()
        
        assert connector.connection is None
        assert connector.cursor is None
        connector.connection.close.assert_called_once()
        connector.cursor.close.assert_called_once()
    
    def test_is_connected(self, connector):
        """Test is_connected method."""
        # When not connected
        assert connector.is_connected() == False
        
        # When connected
        connector.connect()
        assert connector.is_connected() == True
        
        # When connection fails query
        connector.cursor.execute.side_effect = Exception("Connection lost")
        assert connector.is_connected() == False
    
    def test_execute_query_select(self, connector):
        """Test execute_query for SELECT statements."""
        connector.connect()
        
        # Mock fetchall to return some results
        mock_results = [("row1_col1", "row1_col2"), ("row2_col1", "row2_col2")]
        connector.cursor.fetchall.return_value = mock_results
        
        results = connector.execute_query("SELECT * FROM test_table")
        
        assert results == mock_results
        connector.cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        connector.cursor.fetchall.assert_called_once()
    
    def test_execute_query_insert(self, connector):
        """Test execute_query for INSERT statements."""
        connector.connect()
        
        # Mock fetchall to raise pyodbc.Error
        connector.cursor.fetchall.side_effect = MagicMock(side_effect=Exception("Not a query"))
        
        results = connector.execute_query("INSERT INTO test_table VALUES (1, 2)")
        
        assert results is None
        connector.cursor.execute.assert_called_once_with("INSERT INTO test_table VALUES (1, 2)")
        connector.connection.commit.assert_called_once()
    
    def test_insert_many(self, connector):
        """Test insert_many method."""
        connector.connect()
        
        table = "test_table"
        columns = ["col1", "col2"]
        data = [("row1_col1", "row1_col2"), ("row2_col1", "row2_col2")]
        
        result = connector.insert_many(table, columns, data)
        
        assert result == True
        connector.cursor.executemany.assert_called_once()
        connector.connection.commit.assert_called_once()
    
    def test_query_to_dataframe(self, connector):
        """Test query_to_dataframe method."""
        connector.connect()
        
        # Mock pd.read_sql
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        with patch('data_bridge.db_connector.pd.read_sql', return_value=mock_df) as mock_read_sql:
            df = connector.query_to_dataframe("SELECT * FROM test_table")
            
            assert df is mock_df
            mock_read_sql.assert_called_once_with("SELECT * FROM test_table", connector.connection)


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
