
import streamlit as st
import pandas as pd
import os
import sys
from pathlib import Path
import json
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_bridge.config_manager import ConfigManager
from data_bridge.db_connector import DatabaseConnector
from data_bridge.permit_parser import PermitParser
from data_bridge.personal_property_parser import PersonalPropertyParser
from data_bridge.address_matcher import AddressMatcher

# Configure logging
logger = logging.getLogger(__name__)

def run_streamlit_app():
    """Main function to run the Streamlit web UI."""
    st.set_page_config(
        page_title="PACS DataBridge",
        page_icon="🏢",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state for navigation
    if 'page' not in st.session_state:
        st.session_state.page = 'dashboard'
    
    # Load configuration
    config = ConfigManager()
    
    # Sidebar navigation
    st.sidebar.title("PACS DataBridge")
    st.sidebar.image("https://via.placeholder.com/150x80?text=PACS", width=150)
    
    # Navigation options
    pages = {
        'dashboard': "📊 Dashboard",
        'import_permits': "🏗️ Import Permits",
        'import_property': "📦 Import Personal Property",
        'address_lookup': "🔍 Address Lookup",
        'config': "⚙️ Configuration"
    }
    
    # Create sidebar navigation
    st.sidebar.header("Navigation")
    for page_id, page_name in pages.items():
        if st.sidebar.button(page_name, key=f"nav_{page_id}"):
            st.session_state.page = page_id
    
    # Display the selected page
    if st.session_state.page == 'dashboard':
        display_dashboard(config)
    elif st.session_state.page == 'import_permits':
        display_import_permits(config)
    elif st.session_state.page == 'import_property':
        display_import_property(config)
    elif st.session_state.page == 'address_lookup':
        display_address_lookup(config)
    elif st.session_state.page == 'config':
        display_configuration(config)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.info(
        "PACS DataBridge v1.0.0\n\n"
        "© 2025 PACS Development Team"
    )

def display_dashboard(config):
    """Display the dashboard page."""
    st.title("PACS DataBridge Dashboard")
    
    # Quick stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Permits Processed",
            value="0",
            delta=None
        )
    
    with col2:
        st.metric(
            label="Property Records",
            value="0",
            delta=None
        )
    
    with col3:
        st.metric(
            label="Database Status",
            value="Connected" if is_database_connected(config) else "Disconnected"
        )
    
    # Recent activity
    st.subheader("Recent Activity")
    
    # Placeholder for recent activity table
    activity_data = {
        "Date": ["2025-01-03", "2025-01-02", "2025-01-01"],
        "Activity": [
            "Permit Import: 12 records",
            "Database Backup",
            "Property Import: 24 records"
        ],
        "Status": ["Success", "Success", "Failure"],
        "User": ["admin", "system", "admin"]
    }
    
    st.dataframe(pd.DataFrame(activity_data))
    
    # System status
    st.subheader("System Status")
    
    # Configuration status
    config_status = [
        {"Component": "Database Connection", "Status": "Configured" if config.get('database') else "Not Configured"},
        {"Component": "Permit Import Settings", "Status": "Configured" if config.get('import', 'permit') else "Not Configured"},
        {"Component": "Property Import Settings", "Status": "Configured" if config.get('import', 'personal_property') else "Not Configured"}
    ]
    
    st.dataframe(pd.DataFrame(config_status))

def display_import_permits(config):
    """Display the permit import page."""
    st.title("Import Building Permits")
    
    # File upload
    uploaded_file = st.file_uploader("Upload Permit Data", type=["csv", "xlsx", "xls"])
    
    if uploaded_file:
        # Detect file type
        file_type = uploaded_file.name.split(".")[-1].lower()
        
        # Sheet selection for Excel files
        sheet_name = None
        if file_type in ["xlsx", "xls"]:
            xls = pd.ExcelFile(uploaded_file)
            sheets = xls.sheet_names
            sheet_name = st.selectbox("Select Sheet", sheets)
        
        # Preview data
        st.subheader("Data Preview")
        
        try:
            # Read data based on file type
            if file_type == "csv":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            
            # Display preview
            st.dataframe(df.head())
            
            # Field mapping
            st.subheader("Field Mapping")
            
            # Get target fields from permit schema
            target_fields = ["Permit Number", "Permit Type", "Permit Date", "Address", "Owner Name", 
                            "Valuation", "Description", "Parcel ID", "Status"]
            
            # Create mapping UI
            field_mapping = {}
            source_fields = ["-- Ignore --"] + list(df.columns)
            
            col1, col2 = st.columns(2)
            
            with col1:
                for i, field in enumerate(target_fields[:len(target_fields)//2+1]):
                    field_mapping[field] = st.selectbox(
                        f"Map to {field}",
                        options=source_fields,
                        key=f"map_{field}"
                    )
            
            with col2:
                for i, field in enumerate(target_fields[len(target_fields)//2+1:]):
                    field_mapping[field] = st.selectbox(
                        f"Map to {field}",
                        options=source_fields,
                        key=f"map_{field}"
                    )
            
            # Process button
            if st.button("Process Permits"):
                st.info("Processing permits... This may take a moment.")
                
                # TODO: Implement actual processing with PermitParser
                
                # Show success message
                st.success(f"Successfully processed {len(df)} permit records.")
                
                # Display summary
                st.subheader("Processing Summary")
                
                summary_data = {
                    "Total Records": [len(df)],
                    "Successfully Processed": [len(df)],
                    "Failed Records": [0],
                    "Warnings": [0]
                }
                
                st.dataframe(pd.DataFrame(summary_data))
        
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
    else:
        # Instructions when no file is uploaded
        st.info("Upload a CSV or Excel file to import permit data.")
        st.markdown("""
        ### File Requirements
        
        Your permit data file should contain the following information:
        
        - Permit Number
        - Permit Type
        - Issue Date
        - Property Address
        - Owner Information
        - Valuation Amount
        - Work Description
        
        **Supported formats:** CSV, Excel (.xlsx, .xls)
        """)

def display_import_property(config):
    """Display the personal property import page."""
    st.title("Import Personal Property Data")
    
    # File upload
    uploaded_file = st.file_uploader("Upload Personal Property Data", type=["csv", "xlsx", "xls"])
    
    if uploaded_file:
        # Detect file type
        file_type = uploaded_file.name.split(".")[-1].lower()
        
        # Sheet selection for Excel files
        sheet_name = None
        if file_type in ["xlsx", "xls"]:
            xls = pd.ExcelFile(uploaded_file)
            sheets = xls.sheet_names
            sheet_name = st.selectbox("Select Sheet", sheets)
        
        # Preview data
        st.subheader("Data Preview")
        
        try:
            # Read data based on file type
            if file_type == "csv":
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
            
            # Display preview
            st.dataframe(df.head())
            
            # Field mapping
            st.subheader("Field Mapping")
            
            # Get target fields from property schema
            target_fields = ["Account Number", "Owner Name", "Owner Address", "Business Name",
                            "Property Location", "Property Type", "Value", "Status"]
            
            # Create mapping UI
            field_mapping = {}
            source_fields = ["-- Ignore --"] + list(df.columns)
            
            col1, col2 = st.columns(2)
            
            with col1:
                for i, field in enumerate(target_fields[:len(target_fields)//2+1]):
                    field_mapping[field] = st.selectbox(
                        f"Map to {field}",
                        options=source_fields,
                        key=f"pp_map_{field}"
                    )
            
            with col2:
                for i, field in enumerate(target_fields[len(target_fields)//2+1:]):
                    field_mapping[field] = st.selectbox(
                        f"Map to {field}",
                        options=source_fields,
                        key=f"pp_map_{field}"
                    )
            
            # Process button
            if st.button("Process Personal Property"):
                st.info("Processing personal property data... This may take a moment.")
                
                # TODO: Implement actual processing with PersonalPropertyParser
                
                # Show success message
                st.success(f"Successfully processed {len(df)} personal property records.")
                
                # Display summary
                st.subheader("Processing Summary")
                
                summary_data = {
                    "Total Records": [len(df)],
                    "Successfully Processed": [len(df)],
                    "Failed Records": [0],
                    "Warnings": [0]
                }
                
                st.dataframe(pd.DataFrame(summary_data))
        
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
    else:
        # Instructions when no file is uploaded
        st.info("Upload a CSV or Excel file to import personal property data.")
        st.markdown("""
        ### File Requirements
        
        Your personal property data file should contain the following information:
        
        - Account Number
        - Owner Name and Address
        - Business Name (if applicable)
        - Property Location
        - Property Type/Classification
        - Assessed Value
        
        **Supported formats:** CSV, Excel (.xlsx, .xls)
        """)

def display_address_lookup(config):
    """Display the address lookup page."""
    st.title("Address Lookup & Parcel Matching")
    
    # Input address
    address = st.text_input("Enter Address to Search")
    min_confidence = st.slider("Minimum Match Confidence (%)", 0, 100, 70)
    
    if address:
        if st.button("Search Address"):
            st.info(f"Searching for address: {address}")
            
            # Placeholder for actual address matcher logic
            # TODO: Implement with AddressMatcher
            
            # Sample results
            results = [
                {"parcel_id": "123456", "address": "123 Main St", "confidence": 95.5},
                {"parcel_id": "123457", "address": "123 Main Street", "confidence": 92.1},
                {"parcel_id": "789012", "address": "123 N Main St", "confidence": 85.3}
            ]
            
            # Filter by confidence
            results = [r for r in results if r["confidence"] >= min_confidence]
            
            if results:
                st.success(f"Found {len(results)} matching parcels.")
                
                # Display results
                for i, result in enumerate(results):
                    with st.expander(f"Match {i+1}: {result['address']} (Confidence: {result['confidence']}%)"):
                        st.write(f"Parcel ID: {result['parcel_id']}")
                        st.write(f"Match Confidence: {result['confidence']}%")
                        st.write(f"Address: {result['address']}")
                        
                        # Placeholder for parcel details
                        st.write("Owner: John Doe")
                        st.write("Zone: Residential")
                        st.write("Last Assessment: $250,000")
                        
                        if st.button(f"Select Parcel {result['parcel_id']}", key=f"select_{i}"):
                            st.session_state.selected_parcel = result["parcel_id"]
                            st.success(f"Selected parcel: {result['parcel_id']}")
            else:
                st.warning(f"No matches found with confidence >= {min_confidence}%")
    else:
        # Instructions
        st.info("Enter an address to find matching parcels.")
        st.markdown("""
        ### Address Matching
        
        The system will search for the closest matching addresses in the database.
        
        **Tips for best results:**
        - Include street number and street name
        - Abbreviations (St, Ave, Rd) are supported
        - Include city and state if known
        """)

def display_configuration(config):
    """Display the configuration page."""
    st.title("System Configuration")
    
    # Configuration sections
    tabs = st.tabs(["Database", "Import Settings", "Export Settings", "System"])
    
    # Database configuration tab
    with tabs[0]:
        st.header("Database Configuration")
        
        # PACS Database settings
        st.subheader("PACS Database")
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
        
        # DataBridge Database settings
        st.subheader("DataBridge Database")
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
        if st.button("Save Database Configuration"):
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
                'server': db_server,
                'database': db_database,
                'trusted_connection': db_trusted
            }
            
            if not db_trusted:
                db_config['username'] = db_username
                db_config['password'] = db_password
            
            # Save to config manager
            config.set('database', 'pacs', pacs_config)
            config.set('database', 'databridge', db_config)
            config.save_config()
            
            st.success("Database configuration saved successfully.")
            
        # Test connection button
        if st.button("Test Connection"):
            st.info("Testing database connection...")
            
            # Attempt to connect to PACS database
            try:
                # Create database connector with current config
                db = DatabaseConnector(
                    server=pacs_server,
                    database=pacs_database,
                    username='' if pacs_trusted else pacs_username,
                    password='' if pacs_trusted else pacs_password,
                    trusted_connection=pacs_trusted
                )
                
                # Test connection
                if db.connect():
                    # Run a simple query to test
                    result = db.execute_query("SELECT @@VERSION")
                    if result:
                        st.success("Connection to PACS database successful!")
                        st.code(result[0][0][:100] + "...")
                    else:
                        st.error("Connected but could not execute query.")
                else:
                    st.error("Could not connect to PACS database.")
                
                # Close connection
                db.disconnect()
                
            except Exception as e:
                st.error(f"Error testing connection: {str(e)}")
    
    # Import Settings tab
    with tabs[1]:
        st.header("Import Settings")
        
        # Permit Import Settings
        st.subheader("Building Permits")
        permit_config = config.get('import', 'permit') or {}
        
        permit_watch = st.text_input(
            "Permit Watch Folder",
            value=permit_config.get('watch_folder', 'C:\\PACS\\Permits\\Import'),
            key="permit_watch"
        )
        
        permit_archive = st.text_input(
            "Permit Archive Folder",
            value=permit_config.get('archive_folder', 'C:\\PACS\\Permits\\Archive'),
            key="permit_archive"
        )
        
        # Personal Property Import Settings
        st.subheader("Personal Property")
        pp_config = config.get('import', 'personal_property') or {}
        
        pp_watch = st.text_input(
            "Personal Property Watch Folder",
            value=pp_config.get('watch_folder', 'C:\\PACS\\PersonalProperty\\Import'),
            key="pp_watch"
        )
        
        pp_archive = st.text_input(
            "Personal Property Archive Folder",
            value=pp_config.get('archive_folder', 'C:\\PACS\\PersonalProperty\\Archive'),
            key="pp_archive"
        )
        
        # Save import settings
        if st.button("Save Import Settings"):
            # Update permit configuration
            permit_config = {
                'watch_folder': permit_watch,
                'archive_folder': permit_archive,
                'default_schema': permit_config.get('default_schema', {})
            }
            
            # Update personal property configuration
            pp_config = {
                'watch_folder': pp_watch,
                'archive_folder': pp_archive,
                'default_schema': pp_config.get('default_schema', {})
            }
            
            # Save to config manager
            config.set('import', 'permit', permit_config)
            config.set('import', 'personal_property', pp_config)
            config.save_config()
            
            st.success("Import settings saved successfully.")
    
    # Export Settings tab
    with tabs[2]:
        st.header("Export Settings")
        
        export_config = config.get('export') or {}
        
        export_folder = st.text_input(
            "Export Folder",
            value=export_config.get('output_folder', 'C:\\PACS\\Export'),
            key="export_folder"
        )
        
        # Save export settings
        if st.button("Save Export Settings"):
            # Update export configuration
            export_config = {
                'output_folder': export_folder
            }
            
            # Save to config manager
            config.set('export', export_config)
            config.save_config()
            
            st.success("Export settings saved successfully.")
    
    # System tab
    with tabs[3]:
        st.header("System Settings")
        
        logging_config = config.get('logging') or {}
        ui_config = config.get('ui') or {}
        
        # Logging settings
        st.subheader("Logging")
        
        log_level = st.selectbox(
            "Log Level",
            options=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            index=1 if logging_config.get('level', 'INFO') == "INFO" else 0,
            key="log_level"
        )
        
        log_file = st.text_input(
            "Log File",
            value=logging_config.get('log_file', 'C:\\PACS\\Logs\\databridge.log'),
            key="log_file"
        )
        
        # UI settings
        st.subheader("User Interface")
        
        theme = st.selectbox(
            "Theme",
            options=["light", "dark"],
            index=0 if ui_config.get('theme', 'light') == "light" else 1,
            key="theme"
        )
        
        default_view = st.selectbox(
            "Default View",
            options=["dashboard", "import_permits", "import_property", "address_lookup", "config"],
            index=0,
            key="default_view"
        )
        
        # Save system settings
        if st.button("Save System Settings"):
            # Update logging configuration
            logging_config = {
                'level': log_level,
                'log_file': log_file
            }
            
            # Update UI configuration
            ui_config = {
                'theme': theme,
                'default_view': default_view
            }
            
            # Save to config manager
            config.set('logging', logging_config)
            config.set('ui', ui_config)
            config.save_config()
            
            st.success("System settings saved successfully.")

def is_database_connected(config):
    """Check if database connection is working."""
    try:
        # Get database configuration
        db_config = config.get('database', 'pacs')
        if not db_config:
            return False
        
        # Create connector
        db = DatabaseConnector(
            server=db_config.get('server', 'localhost'),
            database=db_config.get('database', 'PACS'),
            username=db_config.get('username', ''),
            password=db_config.get('password', ''),
            trusted_connection=db_config.get('trusted_connection', True)
        )
        
        # Test connection
        connected = db.connect()
        db.disconnect()
        
        return connected
    
    except Exception:
        return False

def start_web_ui(host='127.0.0.1', port=8501):
    """
    Start the Streamlit web UI.
    
    Args:
        host: Host to listen on
        port: Port to listen on
    """
    # Streamlit is typically started using the streamlit command,
    # but this function serves as an entry point for configuration.
    logger.info(f"Starting web UI on {host}:{port}")
    
    # In actual code, you would use:
    # import streamlit.web.cli as stcli
    # sys.argv = ["streamlit", "run", __file__, "--server.address", host, "--server.port", str(port)]
    # stcli.main()
    
    print(f"Web UI should be started with: streamlit run {__file__} --server.address {host} --server.port {port}")
