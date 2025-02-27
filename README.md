# PACS DataBridge

A modern, AI-enhanced data import/export system for building permits and personal property data in the PACS TrueAutomation system. This project aims to replace the legacy CIAPS (County Import and Assessment Processing System) with a flexible, extensible solution.

## Project Overview

PACS DataBridge streamlines county assessment workflows by:

- Intelligently importing and standardizing permit data from various sources
- Processing personal property data with advanced parsing and validation
- Connecting to existing PACS TrueAutomation databases
- Providing an intuitive interface for data mapping and validation

## Key Features

- **Multi-format Import**: Support for CSV, Excel, and other common file formats
- **Intelligent Address Matching**: Advanced algorithms for accurate parcel identification
- **Flexible Mapping**: Configurable field mapping for different data sources
- **Enhanced Validation**: Comprehensive data validation with clear error reporting
- **AI-Powered Classification**: Machine learning for permit type and description classification
- **Seamless Integration**: Non-disruptive integration with existing PACS systems

## System Requirements

- Windows environment
- Python 3.9 or higher
- SQL Server database access
- ODBC Driver 17 for SQL Server
- Appropriate PACS TrueAutomation database permissions

## Installation

1. Clone the repository or download the source code:

```
git clone https://github.com/yourusername/PACS-DataBridge.git
cd PACS-DataBridge
```

2. Create and activate a virtual environment:

```
python -m venv venv
venv\Scripts\activate
```

3. Install the required dependencies:

```
pip install -r requirements.txt
```

4. Configure the database connection:

```
python -m data_bridge.config_manager
```

5. Run tests to ensure everything is working:

```
pytest
```

## Project Structure

```
PACS-DataBridge/
├── src/
│   ├── databridge.py
│   └── data_bridge/
│       ├── __init__.py
│       ├── permit_parser.py
│       ├── personal_property_parser.py
│       ├── db_connector.py
│       ├── config_manager.py
│       ├── address_matcher.py
│       ├── cli.py
│       ├── api.py
│       └── db_setup.py
├── tests/
├── docs/
│   └── roadmap.md
├── config/
│   └── databridge_config.json
├── requirements.txt
└── README.md
```

## Usage

### Command Line Interface

```
# Display help
python src/databridge.py

# Import permits using the CLI
python src/databridge.py cli import-permits "path/to/permits.csv"

# Set up the database
python src/databridge.py db-setup

# Launch the Web API
python src/databridge.py api --host 127.0.0.1 --port 8000

# Launch the Web UI
python src/databridge.py web-ui --host 127.0.0.1 --port 8501
```

### Web UI

PACS DataBridge provides a user-friendly web interface built with Streamlit for easy data import, validation, and configuration. To start the web UI:

```
python src/databridge.py web-ui --host 127.0.0.1 --port 8501
```

The web UI provides the following features:
- Import and validate permit data
- Import and validate personal property data
- Address lookup and parcel matching
- System configuration management

![Web UI Screenshot](docs/web_ui_screenshot.png)

### Web API

The PACS DataBridge system provides a RESTful API for integration with other systems and applications. To start the API server:

```
python src/databridge.py api --host 127.0.0.1 --port 8000
```

#### API Endpoints

- `GET /` - Get API status and information
- `POST /api/import/permits` - Import permit data from a file
- `POST /api/import/property` - Import personal property data from a file
- `POST /api/lookup/parcel` - Look up a parcel by address
- `POST /api/match/address` - Match an address to parcels using fuzzy matching
- `GET /api/config/{section}` - Get a configuration section
- `POST /api/config` - Set a configuration item
- `GET /api/version` - Get the API version

#### Example API Usage

```python
import requests
import json

# API base URL
base_url = "http://127.0.0.1:8000"

# Import permits
response = requests.post(
    f"{base_url}/api/import/permits",
    json={"file_path": "path/to/permits.csv"}
)
print(json.dumps(response.json(), indent=2))

# Match an address
response = requests.post(
    f"{base_url}/api/match/address",
    json={"address": "123 Main St, Anytown, US", "min_confidence": 70.0}
)
print(json.dumps(response.json(), indent=2))
```

For detailed API documentation, navigate to `http://127.0.0.1:8000/docs` when the API server is running.

### Permit Data Import

```python
from data_bridge.permit_parser import PermitParser
from data_bridge.db_connector import PACSConnector

# Initialize the parser
parser = PermitParser()

# Parse permit data
permits = parser.parse_file("path/to/permits.csv")

# Connect to database
db = PACSConnector(server="your_server", database="PACS", trusted_connection=True)
db.connect()

# Process permits
for permit in permits:
    # ... processing logic
```

### Configuration Management

```python
from data_bridge.config_manager import ConfigManager

# Initialize config manager
config = ConfigManager()

# Import from existing CIAPS config
config.setup_from_ciaps("path/to/CIAPS_Loading.txt")

# Access configuration
db_server = config.get("database", "pacs.server")
```

## Development Roadmap

1. **Phase 1: Core Framework** (Complete)
   - ✓ Permit data parser
   - ✓ Database connection module
   - ✓ Configuration management
   - ✓ Address matching service

2. **Phase 2: Web Interface** (Current)
   - ✓ API backend with FastAPI
   - ✓ Streamlit-based user interface
   - Interactive data mapping
   - Validation dashboard

3. **Phase 3: Advanced Features**
   - Machine learning integration
   - Batch processing
   - Reporting and analytics
   - Export functionality

4. **Phase 4: Enterprise Features**
   - Multi-county support
   - Workflow automation
   - Scheduled imports
   - High-volume optimization

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- PACS TrueAutomation documentation
- Legacy CIAPS system architecture
- County assessment offices for requirements and testing
