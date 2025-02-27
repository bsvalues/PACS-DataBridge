"""
Web UI Module for PACS DataBridge

Provides a Streamlit-based web interface for the PACS DataBridge system.
Enables user-friendly access to system functionality including data import, 
validation, visualization, and configuration.
"""

import os
import sys
import json
import tempfile
import logging
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from data_bridge import __version__
from data_bridge.config_manager import ConfigManager
from data_bridge.permit_parser import PermitParser
from data_bridge.personal_property_parser import PersonalPropertyParser
from data_bridge.db_connector import DatabaseConnector, PACSConnector
from data_bridge.address_matcher import AddressMatcher, Address

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# App title and configuration
st.set_page_config(
    page_title="PACS DataBridge",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state variables if they don't exist
if 'db_connector' not in st.session_state:
    st.session_state.db_connector = None
if 'address_matcher' not in st.session_state:
    st.session_state.address_matcher = None
if 'config' not in st.session_state:
    st.session_state.config = ConfigManager()
if 'imported_data' not in st.session_state:
    st.session_state.imported_data = None
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None


def init_database_connection():
    """Initialize database connection."""
    config = st.session_state.config
    pacs_config = config.get('database', 'pacs')
    
    if not pacs_config:
        st.warning("Database connection not configured. Please set up configuration.")
        return False
    
    try:
        st.session_state.db_connector = PACSConnector(
            server=pacs_config.get('server', 'localhost'),
            database=pacs_config.get('database', 'PACS'),
            username=pacs_config.get('username', ''),
            password=pacs_config.get('password', ''),
            trusted_connection=pacs_config.get('trusted_connection', True)
        )
        
        # Test connection
        if st.session_state.db_connector.connect():
            st.session_state.address_matcher = AddressMatcher(st.session_state.db_connector)
            st.session_state.db_connector.disconnect()
            return True
        else:
            st.error("Failed to connect to database. Please check your configuration.")
            return False
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")
        return False


def render_sidebar():
    """Render sidebar with navigation."""
    with st.sidebar:
        st.title("PACS DataBridge")
        st.caption(f"v{__version__}")
        
        # Navigation
        st.header("Navigation")
        page = st.radio(
            "Select Page",
            ["Import Permits", "Import Property", "Address Lookup", "Configuration"]
        )
        
        # Database connection status
        st.header("Database Status")
        conn_status = "Not Connected"
        if st.session_state.db_connector:
            try:
                if st.session_state.db_connector.connect():
                    conn_status = "Connected ✅"
                    st.session_state.db_connector.disconnect()
                else:
                    conn_status = "Connection Failed ❌"
            except:
                conn_status = "Connection Error ❌"
        
        st.info(f"Database: {conn_status}")
        if st.button("Test Connection"):
            if init_database_connection():
                st.success("Database connection successful!")
            else:
                st.error("Database connection failed!")
        
        st.divider()
        st.caption("© PACS DataBridge")
    
    return page


def render_import_permits():
    """Render the permit import page."""
    st.header("Import Permits")
    st.write("Upload building permit data for processing and import into PACS.")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Permit Data File",
        type=["csv", "xlsx", "xls"],
        help="Upload CSV or Excel file containing permit data"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        sheet_name = st.text_input("Sheet Name (for Excel)", value="Sheet1")
        
    with col2:
        min_confidence = st.slider(
            "Minimum Address Match Confidence (%)",
            min_value=0,
            max_value=100,
            value=70,
            help="Minimum confidence level required for address matching"
        )
    
    if uploaded_file is not None:
        # Save uploaded file to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name
        
        try:
            if st.button("Process Permits"):
                if not st.session_state.address_matcher:
                    if not init_database_connection():
                        st.warning("Please configure database connection first.")
                        return
                
                with st.spinner("Processing permit data..."):
                    # Initialize parser
                    parser = PermitParser(address_matcher=st.session_state.address_matcher)
                    
                    # Parse data
                    if Path(temp_path).suffix.lower() in ['.xlsx', '.xls']:
                        df = parser.parse_file(temp_path, sheet_name=sheet_name)
                    else:
                        df = parser.parse_file(temp_path)
                    
                    # Store in session state
                    st.session_state.imported_data = df
                    
                    # Display results
                    st.success(f"Successfully processed {len(df)} records")
                    
                    # Calculate stats
                    error_count = 0
                    if 'validation_errors' in df.columns:
                        error_count = df['validation_errors'].apply(lambda x: x != '' and not pd.isna(x)).sum()
                    
                    # Display stats
                    st.metric("Total Records", len(df))
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Valid Records", len(df) - error_count)
                    with col2:
                        st.metric("Records with Errors", error_count)
                    
                    # Show data preview
                    st.subheader("Data Preview")
                    st.dataframe(df.head(10))
                    
                    # Export options
                    st.subheader("Export Options")
                    export_path = st.text_input(
                        "Export Path",
                        value=f"processed_permits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
                    
                    if st.button("Export Processed Data"):
                        df.to_csv(export_path, index=False)
                        st.success(f"Data exported to {export_path}")
        
        except Exception as e:
            st.error(f"Error processing permits: {str(e)}")
        
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
            except:
                pass


def render_import_property():
    """Render the personal property import page."""
    st.header("Import Personal Property")
    st.write("Upload personal property data for processing and import into PACS.")
    
    # File upload
    uploaded_file = st.file_uploader(
        "Upload Personal Property Data File",
        type=["csv", "xlsx", "xls"],
        help="Upload CSV or Excel file containing personal property data"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        sheet_name = st.text_input("Sheet Name (for Excel)", value="Sheet1")
        
    with col2:
        min_confidence = st.slider(
            "Minimum Address Match Confidence (%)",
            min_value=0,
            max_value=100,
            value=70,
            help="Minimum confidence level required for address matching"
        )
    
    if uploaded_file is not None:
        # Save uploaded file to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name
        
        try:
            if st.button("Process Personal Property"):
                if not st.session_state.address_matcher:
                    if not init_database_connection():
                        st.warning("Please configure database connection first.")
                        return
                
                with st.spinner("Processing personal property data..."):
                    # Initialize parser
                    parser = PersonalPropertyParser(address_matcher=st.session_state.address_matcher)
                    
                    # Parse data
                    if Path(temp_path).suffix.lower() in ['.xlsx', '.xls']:
                        df = parser.parse_file(temp_path, sheet_name=sheet_name)
                    else:
                        df = parser.parse_file(temp_path)
                    
                    # Store in session state
                    st.session_state.imported_data = df
                    
                    # Display results
                    st.success(f"Successfully processed {len(df)} records")
                    
                    # Calculate stats
                    error_count = 0
                    if 'validation_errors' in df.columns:
                        error_count = df['validation_errors'].apply(lambda x: x != '' and not pd.isna(x)).sum()
                    
                    # Display stats
                    st.metric("Total Records", len(df))
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Valid Records", len(df) - error_count)
                    with col2:
                        st.metric("Records with Errors", error_count)
                    
                    # Show data preview
                    st.subheader("Data Preview")
                    st.dataframe(df.head(10))
                    
                    # Export options
                    st.subheader("Export Options")
                    export_path = st.text_input(
                        "Export Path",
                        value=f"processed_property_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    )
                    
                    if st.button("Export Processed Data"):
                        df.to_csv(export_path, index=False)
                        st.success(f"Data exported to {export_path}")
        
        except Exception as e:
            st.error(f"Error processing personal property: {str(e)}")
        
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
            except:
                pass


def render_address_lookup():
    """Render the address lookup page."""
    st.header("Address Lookup")
    st.write("Look up parcels by address using fuzzy matching.")
    
    address = st.text_input("Enter Address", placeholder="123 Main St, Anytown, US")
    
    col1, col2 = st.columns(2)
    with col1:
        min_confidence = st.slider(
            "Minimum Confidence (%)",
            min_value=0,
            max_value=100,
            value=70,
            help="Minimum confidence level required for address matching"
        )
    
    with col2:
        lookup_type = st.radio(
            "Lookup Type",
            ["Fuzzy Match", "Direct Database Lookup"]
        )
    
    if st.button("Look Up Address"):
        if not address:
            st.warning("Please enter an address to look up.")
            return
        
        if not st.session_state.address_matcher or not st.session_state.db_connector:
            if not init_database_connection():
                st.warning("Please configure database connection first.")
                return
        
        with st.spinner("Looking up address..."):
            try:
                if lookup_type == "Fuzzy Match":
                    # Use address matcher for fuzzy matching
                    matches = st.session_state.address_matcher.match_address_to_parcel(
                        address,
                        min_confidence=min_confidence
                    )
                    
                    if not matches:
                        st.info("No matching parcels found.")
                        return
                    
                    # Display results
                    st.success(f"Found {len(matches)} matching parcels")
                    
                    # Create results dataframe
                    results_df = pd.DataFrame(matches)
                    
                    # Display results table
                    st.dataframe(results_df)
                
                else:  # Direct Database Lookup
                    # Connect to database
                    if st.session_state.db_connector.connect():
                        parcels = st.session_state.db_connector.get_parcel_by_address(address)
                        st.session_state.db_connector.disconnect()
                        
                        if not parcels:
                            st.info("No matching parcels found.")
                            return
                        
                        # Display results
                        st.success(f"Found {len(parcels)} matching parcels")
                        
                        # Create results dataframe
                        results_df = pd.DataFrame(parcels)
                        
                        # Display results table
                        st.dataframe(results_df)
                    else:
                        st.error("Failed to connect to database. Please check your configuration.")
            
            except Exception as e:
                st.error(f"Error looking up address: {str(e)}")


def render_configuration():
    """Render the configuration page."""
    st.header("Configuration")
    st.write("Configure database connections and system settings.")
    
    # Get current configuration
    config = st.session_state.config
    
    # Database configuration
    st.subheader("Database Configuration")
    
    # PACS database
    st.markdown("##### PACS Database")
    pacs_config = config.get('database', 'pacs') or {}
    
    col1, col2 = st.columns(2)
    with col1:
        pacs_server = st.text_input(
            "Server",
            value=pacs_config.get('server', 'localhost'),
            key="pacs_server"
        )
        pacs_database = st.text_input(
            "Database",
            value=pacs_config.get('database', 'PACS'),
            key="pacs_database"
        )
    
    with col2:
        pacs_trusted = st.checkbox(
            "Use Trusted Connection",
            value=pacs_config.get('trusted_connection', True),
            key="pacs_trusted"
        )
        
        pacs_username = st.text_input(
            "Username",
            value=pacs_config.get('username', ''),
            disabled=pacs_trusted,
            key="pacs_username"
        )
        
        pacs_password = st.text_input(
            "Password",
            value=pacs_config.get('password', ''),
            type="password",
            disabled=pacs_trusted,
            key="pacs_password"
        )
    
    # DataBridge database
    st.markdown("##### DataBridge Database")
    db_config = config.get('database', 'databridge') or {}
    
    col1, col2 = st.columns(2)
    with col1:
        db_server = st.text_input(
            "Server",
            value=db_config.get('server', 'localhost'),
            key="db_server"
        )
        db_database = st.text_input(
            "Database",
            value=db_config.get('database', 'DataBridge'),
            key="db_database"
        )
    
    with col2:
        db_trusted = st.checkbox(
            "Use Trusted Connection",
            value=db_config.get('trusted_connection', True),
            key="db_trusted"
        )
        
        db_username = st.text_input(
            "Username",
            value=db_config.get('username', ''),
            disabled=db_trusted,
            key="db_username"
        )
        
        db_password = st.text_input(
            "Password",
            value=db_config.get('password', ''),
            type="password",
            disabled=db_trusted,
            key="db_password"
        )
    
    # Save button
    if st.button("Save Configuration"):
        # Update PACS configuration
        pacs_config = {
            'server': pacs_server,
            'database': pacs_database,
            'trusted_connection': pacs_trusted
        }
        
        if not pacs_trusted:
            pacs_config['username'] = pacs_username
            pacs_config['password'] = pacs_password
        
        # Update DataBridge configuration
        db_config = {
            'server': db_server, Database")
    db_config = config.get('database', 'databridge') or {}
    
    col1, col2 = st.columns(2)
    with col1:
        db_server = st.text_input(
            "Server",
            value=db_config.get('server', 'localhost'),
            key="db_server"
        )
        db_database = st.text_input(
            "Database",
            value=db_config.get('database', 'DataBridge'),
            key="db_database"
        )
    
    with col2:
        db_trusted = st.checkbox(
            "Use Trusted Connection",
            value=db_config.get('trusted_connection', True),
            key="db_trusted"
        )
        
        db_username = st.text_input(
            "Username",
            value=db_config.get('username', ''),
            disabled=db_trusted,
            key="db_username"
        )
        
        db_password = st.text_input(
            "Password",
            value=db_config.get('password', ''),
            type="password",
            disabled=db_trusted,
            key="db_password"
        )
    
    # Save configuration
    if st.button("Save Configuration"):
        # PACS database config
        pacs_config = {
            'server': pacs_server,
            'database': pacs_database,
            'trusted_connection': pacs_trusted
        }
        
        if not pacs_trusted:
            pacs_config['username'] = pacs_username
            pacs_config['password'] = pacs_password
        
        # DataBridge database config
        db_config = {
            'server': db_server,
            'database': db_database,
            'trusted_connection': db_trusted
        }
        
        if not db_trusted:
            db_config['username'] = db_username
            db_config['password'] = db_password
        
        # Set configuration
        config.set('database', 'pacs', pacs_config)
        config.set('database', 'databridge', db_config)
        
        # Save configuration
        if config.save_config():
            st.success("Configuration saved successfully.")
            
            # Test connection with new configuration
            init_database_connection()
        else:
            st.error("Failed to save configuration.")
    
    # CIAPS configuration import
    st.subheader("Import from CIAPS")
    ciaps_path = st.text_input("CIAPS Configuration Path", key="ciaps_path")
    
    if st.button("Import from CIAPS") and ciaps_path:
        if os.path.exists(ciaps_path):
            if config.setup_from_ciaps(ciaps_path):
                st.success("CIAPS configuration imported successfully.")
            else:
                st.error("Failed to import CIAPS configuration.")
        else:
            st.error(f"File not found: {ciaps_path}")
    
    # Export/import configuration
    st.subheader("Export/Import Configuration")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Export Configuration"):
            export_path = "exported_config.json"
            with open(export_path, 'w') as f:
                json.dump(config.config, f, indent=2)
            st.success(f"Configuration exported to {export_path}")
    
    with col2:
        uploaded_file = st.file_uploader(
            "Import Configuration",
            type=["json"],
            help="Upload JSON configuration file"
        )
        
        if uploaded_file is not None:
            try:
                config_data = json.loads(uploaded_file.getvalue())
                config.config = config_data
                config.save_config()
                st.success("Configuration imported successfully.")
            except Exception as e:
                st.error(f"Error importing configuration: {str(e)}")


def main():
    """Main Streamlit application."""
    # Render sidebar and get selected page
    page = render_sidebar()
    
    # Render selected page
    if page == "Import Permits":
        render_import_permits()
    elif page == "Import Property":
        render_import_property()
    elif page == "Address Lookup":
        render_address_lookup()
    elif page == "Configuration":
        render_configuration()


if __name__ == "__main__":
    main()
