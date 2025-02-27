"""
Configuration Manager Module

Provides functionality to manage configuration settings for the PACS DataBridge system,
including database connections, file paths, and integration settings.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
import configparser
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Configuration management for the PACS DataBridge system.
    Handles loading, saving, and accessing configuration settings.
    """
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_dir: Directory containing configuration files,
                        defaults to config/ in the application root
        """
        # Set default config directory if none provided
        if config_dir is None:
            # Assume config is in a config folder at the project root
            self.config_dir = Path(__file__).resolve().parent.parent.parent / 'config'
        else:
            self.config_dir = Path(config_dir)
            
        # Create the config directory if it doesn't exist
        os.makedirs(self.config_dir, exist_ok=True)
        
        # Default configuration filename
        self.config_file = self.config_dir / 'databridge_config.json'
        
        # Initialize configuration dictionary
        self.config: Dict[str, Any] = {
            'database': {
                'pacs': {
                    'server': 'localhost',
                    'database': 'PACS',
                    'trusted_connection': True,
                    'username': '',
                    'password': ''
                },
                'databridge': {
                    'server': 'localhost',
                    'database': 'DataBridge',
                    'trusted_connection': True,
                    'username': '',
                    'password': ''
                }
            },
            'import': {
                'permit': {
                    'default_schema': {},
                    'watch_folder': '',
                    'archive_folder': ''
                },
                'personal_property': {
                    'default_schema': {},
                    'watch_folder': '',
                    'archive_folder': ''
                }
            },
            'export': {
                'output_folder': ''
            },
            'logging': {
                'level': 'INFO',
                'log_file': ''
            },
            'ui': {
                'theme': 'light',
                'default_view': 'dashboard'
            }
        }
        
        # Load configuration if file exists
        if os.path.exists(self.config_file):
            self.load_config()
        else:
            logger.info(f"No configuration file found at {self.config_file}. Using defaults.")
            self.save_config()
    
    def load_config(self) -> bool:
        """
        Load configuration from the JSON file.
        
        Returns:
            True if configuration loaded successfully, False otherwise
        """
        try:
            with open(self.config_file, 'r') as f:
                loaded_config = json.load(f)
                
            # Update configuration with loaded values
            # This preserves the structure of the default config
            def update_dict(target, source):
                for key, value in source.items():
                    if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                        update_dict(target[key], value)
                    else:
                        target[key] = value
            
            update_dict(self.config, loaded_config)
            logger.info(f"Configuration loaded from {self.config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return False
    
    def save_config(self) -> bool:
        """
        Save the current configuration to the JSON file.
        
        Returns:
            True if configuration saved successfully, False otherwise
        """
        try:
            # Remove sensitive information before saving
            sanitized_config = self._sanitize_config_for_saving()
            
            with open(self.config_file, 'w') as f:
                json.dump(sanitized_config, f, indent=4)
                
            logger.info(f"Configuration saved to {self.config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            return False
    
    def _sanitize_config_for_saving(self) -> Dict[str, Any]:
        """
        Create a copy of the configuration with sensitive information masked.
        
        Returns:
            Sanitized configuration dictionary
        """
        # Deep copy the configuration
        import copy
        sanitized = copy.deepcopy(self.config)
        
        # Mask database passwords if they exist
        for db_section in ['pacs', 'databridge']:
            if db_section in sanitized['database']:
                if 'password' in sanitized['database'][db_section] and sanitized['database'][db_section]['password']:
                    sanitized['database'][db_section]['password'] = '********'
        
        return sanitized
    
    def get(self, section: str, key: Optional[str] = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            section: Configuration section (e.g., 'database', 'import')
            key: Optional key within the section
            
        Returns:
            Configuration value or None if not found
        """
        if section not in self.config:
            return None
            
        if key is None:
            return self.config[section]
            
        # Handle nested keys with dot notation (e.g., 'database.pacs.server')
        if '.' in key:
            parts = key.split('.')
            value = self.config[section]
            for part in parts:
                if part in value:
                    value = value[part]
                else:
                    return None
            return value
            
        # Simple key
        if key in self.config[section]:
            return self.config[section][key]
            
        return None
    
    def set(self, section: str, key: str, value: Any) -> bool:
        """
        Set a configuration value.
        
        Args:
            section: Configuration section
            key: Key within the section (can use dot notation for nested keys)
            value: Value to set
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create section if it doesn't exist
            if section not in self.config:
                self.config[section] = {}
                
            # Handle nested keys with dot notation
            if '.' in key:
                parts = key.split('.')
                config_section = self.config[section]
                
                # Navigate to the correct nested dictionary
                for part in parts[:-1]:
                    if part not in config_section:
                        config_section[part] = {}
                    config_section = config_section[part]
                    
                # Set the value
                config_section[parts[-1]] = value
            else:
                # Simple key
                self.config[section][key] = value
                
            return True
            
        except Exception as e:
            logger.error(f"Error setting configuration value: {str(e)}")
            return False
    
    def import_from_ini(self, ini_file: str) -> bool:
        """
        Import configuration from an INI file (for compatibility with older systems).
        
        Args:
            ini_file: Path to the INI file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(ini_file):
                logger.error(f"INI file not found: {ini_file}")
                return False
                
            parser = configparser.ConfigParser()
            parser.read(ini_file)
            
            # Convert INI structure to our configuration format
            for section in parser.sections():
                if section not in self.config:
                    self.config[section] = {}
                    
                for key, value in parser[section].items():
                    # Try to determine the value type
                    if value.lower() in ('true', 'yes', 'on'):
                        self.config[section][key] = True
                    elif value.lower() in ('false', 'no', 'off'):
                        self.config[section][key] = False
                    elif value.isdigit():
                        self.config[section][key] = int(value)
                    elif value.replace('.', '', 1).isdigit() and value.count('.') < 2:
                        self.config[section][key] = float(value)
                    else:
                        self.config[section][key] = value
            
            logger.info(f"Configuration imported from INI file: {ini_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing configuration from INI file: {str(e)}")
            return False
    
    def setup_from_ciaps(self, ciaps_config_file: str) -> bool:
        """
        Import configuration from existing CIAPS configuration file.
        
        Args:
            ciaps_config_file: Path to the CIAPS configuration file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(ciaps_config_file):
                logger.error(f"CIAPS configuration file not found: {ciaps_config_file}")
                return False
            
            # Read the CIAPS configuration file
            with open(ciaps_config_file, 'r') as f:
                lines = f.readlines()
            
            # Parse CIAPS connection string
            conn_str = None
            for line in lines:
                if line.strip().startswith('ConnStr='):
                    conn_str = line.strip()[8:].strip()  # Remove 'ConnStr=' prefix
                    break
            
            if not conn_str:
                logger.error("No connection string found in CIAPS configuration")
                return False
            
            # Parse the connection string components
            conn_parts = {}
            for part in conn_str.split(';'):
                if '=' in part:
                    key, value = part.split('=', 1)
                    conn_parts[key.strip()] = value.strip()
            
            # Extract database connection information
            if 'SERVER' in conn_parts:
                self.set('database', 'pacs.server', conn_parts['SERVER'])
            
            if 'DATABASE' in conn_parts:
                self.set('database', 'pacs.database', conn_parts['DATABASE'])
            
            if 'UID' in conn_parts:
                self.set('database', 'pacs.username', conn_parts['UID'])
                self.set('database', 'pacs.trusted_connection', False)
            
            if 'PWD' in conn_parts:
                self.set('database', 'pacs.password', conn_parts['PWD'])
            
            # Save the configuration
            self.save_config()
            
            logger.info(f"Configuration imported from CIAPS file: {ciaps_config_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing configuration from CIAPS file: {str(e)}")
            return False


# Example usage
if __name__ == "__main__":
    # Create a configuration manager
    config = ConfigManager()
    
    # Set some configuration values
    config.set('database', 'pacs.server', 'sql-server-01')
    config.set('import', 'permit.watch_folder', 'C:\\PACS\\Permits\\Import')
    
    # Save the configuration
    config.save_config()
    
    # Get configuration values
    db_server = config.get('database', 'pacs.server')
    print(f"PACS Database Server: {db_server}")
    
    # Import from CIAPS config (if available)
    ciaps_config = "H:\\Projects\\CIAPS\\CIAPS_Loading.txt"
    if os.path.exists(ciaps_config):
        config.setup_from_ciaps(ciaps_config)
        print("Imported configuration from CIAPS")
"""
Configuration manager for PACS DataBridge.
"""

import os
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Configuration manager for the PACS DataBridge system.
    Handles loading, saving, and accessing configuration settings.
    """
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Optional path to configuration file
        """
        # Default config path is in the config directory
        if config_path is None:
            self.config_path = Path(__file__).resolve().parent.parent.parent / 'config' / 'databridge_config.json'
        else:
            self.config_path = Path(config_path)
        
        # Initialize empty config
        self.config = {}
        
        # Try to load configuration
        self.load_config()
    
    def load_config(self) -> bool:
        """
        Load configuration from file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.config_path.exists():
                logger.warning(f"Configuration file not found: {self.config_path}")
                self._create_default_config()
                return False
            
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            
            logger.info(f"Configuration loaded from {self.config_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            self._create_default_config()
            return False
    
    def save_config(self) -> bool:
        """
        Save configuration to file.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(self.config_path.parent, exist_ok=True)
            
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=4)
            
            logger.info(f"Configuration saved to {self.config_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving configuration: {str(e)}")
            return False
    
    def get(self, section: str, key: Optional[str] = None) -> Any:
        """
        Get configuration value.
        
        Args:
            section: Configuration section
            key: Optional key within section
        
        Returns:
            Configuration value, section dict, or None if not found
        """
        if section not in self.config:
            return None
        
        if key is None:
            return self.config[section]
        
        if key not in self.config[section]:
            return None
        
        return self.config[section][key]
    
    def set(self, section: str, key: str, value: Any) -> None:
        """
        Set configuration value.
        
        Args:
            section: Configuration section
            key: Key within section
            value: Value to set
        """
        if section not in self.config:
            self.config[section] = {}
        
        self.config[section][key] = value
    
    def _create_default_config(self) -> None:
        """Create default configuration."""
        self.config = {
            "database": {
                "pacs": {
                    "server": "localhost",
                    "database": "PACS",
                    "trusted_connection": True,
                    "username": "",
                    "password": ""
                },
                "databridge": {
                    "server": "localhost",
                    "database": "DataBridge",
                    "trusted_connection": True,
                    "username": "",
                    "password": ""
                }
            },
            "import": {
                "permit": {
                    "default_schema": {},
                    "watch_folder": "C:\\PACS\\Permits\\Import",
                    "archive_folder": "C:\\PACS\\Permits\\Archive"
                },
                "personal_property": {
                    "default_schema": {},
                    "watch_folder": "C:\\PACS\\PersonalProperty\\Import",
                    "archive_folder": "C:\\PACS\\PersonalProperty\\Archive"
                }
            },
            "export": {
                "output_folder": "C:\\PACS\\Export"
            },
            "logging": {
                "level": "INFO",
                "log_file": "C:\\PACS\\Logs\\databridge.log"
            },
            "ui": {
                "theme": "light",
                "default_view": "dashboard"
            }
        }
        
        # Try to save default config
        self.save_config()
    
    def setup_from_ciaps(self, ciaps_config_path: Union[str, Path]) -> bool:
        """
        Set up configuration from existing CIAPS configuration.
        
        Args:
            ciaps_config_path: Path to CIAPS configuration file
        
        Returns:
            True if successful, False otherwise
        """
        try:
            ciaps_config_path = Path(ciaps_config_path)
            
            if not ciaps_config_path.exists():
                logger.error(f"CIAPS config file not found: {ciaps_config_path}")
                return False
            
            # Parse CIAPS configuration file
            ciaps_config = self._parse_ciaps_config(ciaps_config_path)
            
            if not ciaps_config:
                logger.error("Failed to parse CIAPS configuration")
                return False
            
            # Map CIAPS settings to DataBridge settings
            self._map_ciaps_settings(ciaps_config)
            
            logger.info(f"Configuration imported from CIAPS: {ciaps_config_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing CIAPS configuration: {str(e)}")
            return False
    
    def _parse_ciaps_config(self, ciaps_config_path: Path) -> Dict[str, str]:
        """
        Parse CIAPS configuration file.
        
        Args:
            ciaps_config_path: Path to CIAPS configuration file
        
        Returns:
            Dictionary of CIAPS settings
        """
        ciaps_config = {}
        
        try:
            with open(ciaps_config_path, 'r') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith(';') or line.startswith('#'):
                    continue
                
                # Parse key-value pairs
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    
                    ciaps_config[key] = value
            
            return ciaps_config
            
        except Exception as e:
            logger.error(f"Error parsing CIAPS config file: {str(e)}")
            return {}
    
    def _map_ciaps_settings(self, ciaps_config: Dict[str, str]) -> None:
        """
        Map CIAPS settings to DataBridge settings.
        
        Args:
            ciaps_config: Dictionary of CIAPS settings
        """
        # Map database settings
        if 'SQLServer' in ciaps_config:
            self.set('database', 'pacs', {
                'server': ciaps_config.get('SQLServer', 'localhost'),
                'database': ciaps_config.get('Database', 'PACS'),
                'trusted_connection': True,
                'username': '',
                'password': ''
            })
        
        # Map import settings
        if 'ImportFolder' in ciaps_config:
            import_folder = ciaps_config['ImportFolder']
            self.set('import', 'permit', {
                'default_schema': {},
                'watch_folder': import_folder,
                'archive_folder': os.path.join(import_folder, 'Archive')
            })
        
        # Map export settings
        if 'ExportFolder' in ciaps_config:
            self.set('export', 'output_folder', ciaps_config['ExportFolder'])
        
        # Map other settings as needed
