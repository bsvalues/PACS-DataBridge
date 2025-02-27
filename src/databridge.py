#!/usr/bin/env python3
"""
PACS DataBridge - Main Entry Point

This script serves as the main entry point for the PACS DataBridge system.
It provides a way to launch the various components of the system, including
the CLI tool, database setup, and web interfaces.
"""

import os
import sys
import argparse
import logging
import subprocess
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_bridge import __version__
from data_bridge.cli import main as cli_main
from data_bridge.db_setup import main as db_setup_main
from data_bridge.api import start_api

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def start_web_ui(host="127.0.0.1", port=8501):
    """Start the Streamlit web UI."""
    web_ui_path = Path(__file__).resolve().parent / "data_bridge" / "web_ui.py"
    
    # Build the command to run Streamlit
    command = [
        "streamlit", "run",
        str(web_ui_path),
        "--server.address", host,
        "--server.port", str(port)
    ]
    
    # Run the command
    try:
        logger.info(f"Starting Streamlit web UI on {host}:{port}")
        subprocess.run(command)
    except Exception as e:
        logger.error(f"Error starting Streamlit web UI: {str(e)}")
        sys.exit(1)

def main():
    """Main entry point for the PACS DataBridge system."""
    # Create argument parser
    parser = argparse.ArgumentParser(
        description=f"PACS DataBridge v{__version__}",
        epilog="A modern, AI-enhanced data import/export system for PACS TrueAutomation"
    )
    
    # Add subparsers for components
    subparsers = parser.add_subparsers(dest='component', help='Component to launch')
    
    # CLI component
    cli_parser = subparsers.add_parser('cli', help='Launch the command-line interface')
    cli_parser.add_argument('cli_args', nargs='*', help='Arguments to pass to the CLI')
    
    # Database setup component
    db_parser = subparsers.add_parser('db-setup', help='Set up the database')
    
    # Web API component
    api_parser = subparsers.add_parser('api', help='Launch the web API')
    api_parser.add_argument('--host', default='127.0.0.1', help='Host to listen on')
    api_parser.add_argument('--port', type=int, default=8000, help='Port to listen on')
    
    # Web UI component
    web_parser = subparsers.add_parser('web-ui', help='Launch the Streamlit web UI')
    web_parser.add_argument('--host', default='127.0.0.1', help='Host to listen on')
    web_parser.add_argument('--port', type=int, default=8501, help='Port to listen on')
    
    # Version command
    version_parser = subparsers.add_parser('version', help='Show version information')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Launch appropriate component
    if args.component == 'cli':
        # Pass remaining arguments to CLI
        sys.argv = [sys.argv[0]] + args.cli_args
        cli_main()
    elif args.component == 'db-setup':
        db_setup_main()
    elif args.component == 'api':
        logger.info(f"Starting web API on {args.host}:{args.port}")
        start_api(host=args.host, port=args.port)
    elif args.component == 'web-ui':
        start_web_ui(host=args.host, port=args.port)
    elif args.component == 'version':
        print(f"PACS DataBridge v{__version__}")
        print("A modern, AI-enhanced data import/export system for PACS TrueAutomation")
    else:
        # Show help if no component specified
        parser.print_help()


if __name__ == "__main__":
    main()
