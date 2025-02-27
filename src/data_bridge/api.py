"""
Web API Module

Provides a FastAPI-based web API for the PACS DataBridge system.
Enables programmatic access to the system's functionality, including
data import, validation, and configuration.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Query, Form, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

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

# Create FastAPI application
app = FastAPI(
    title="PACS DataBridge API",
    description="API for the PACS DataBridge system",
    version=__version__
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins in development
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Pydantic models for request/response
class ImportRequest(BaseModel):
    file_path: str
    format_type: Optional[str] = None
    sheet_name: Optional[str] = None

class ParcelLookupRequest(BaseModel):
    address: str
    min_confidence: Optional[float] = 70.0

class ParcelInfo(BaseModel):
    parcel_id: str
    parcel_address: str
    confidence: Optional[float] = None
    input_address: Optional[str] = None

class ImportResult(BaseModel):
    status: str
    message: str
    record_count: int
    valid_count: int
    error_count: int
    sample_record: Optional[Dict[str, Any]] = None
    output_path: Optional[str] = None

class ConfigItem(BaseModel):
    section: str
    key: str
    value: Any

class StatusResponse(BaseModel):
    status: str
    version: str
    database_connected: bool
    config_loaded: bool

# Dependency to get database connector
async def get_db_connector():
    """
    Get a database connector as a dependency.
    """
    config = ConfigManager()
    pacs_config = config.get('database', 'pacs')
    
    if not pacs_config:
        return None
    
    connector = PACSConnector(
        server=pacs_config.get('server', 'localhost'),
        database=pacs_config.get('database', 'PACS'),
        username=pacs_config.get('username', ''),
        password=pacs_config.get('password', ''),
        trusted_connection=pacs_config.get('trusted_connection', True)
    )
    
    # Test connection
    if connector.connect():
        yield connector
        connector.disconnect()
    else:
        yield None

# Dependency to get address matcher
async def get_address_matcher(db_connector: Optional[PACSConnector] = Depends(get_db_connector)):
    """
    Get an address matcher as a dependency.
    """
    return AddressMatcher(db_connector)

# API routes
@app.get("/", tags=["General"])
async def root():
    """Get API status and information."""
    config = ConfigManager()
    
    # Test database connection
    db_connected = False
    db_connector = None
    
    try:
        pacs_config = config.get('database', 'pacs')
        if pacs_config:
            db_connector = PACSConnector(
                server=pacs_config.get('server', 'localhost'),
                database=pacs_config.get('database', 'PACS'),
                username=pacs_config.get('username', ''),
                password=pacs_config.get('password', ''),
                trusted_connection=pacs_config.get('trusted_connection', True)
            )
            db_connected = db_connector.connect()
            if db_connected:
                db_connector.disconnect()
    except Exception as e:
        logger.error(f"Error testing database connection: {str(e)}")
    
    return StatusResponse(
        status="ok",
        version=__version__,
        database_connected=db_connected,
        config_loaded=config.config_file.exists()
    )

@app.post("/api/import/permits", response_model=ImportResult, tags=["Import"])
async def import_permits(
    request: ImportRequest,
    background_tasks: BackgroundTasks,
    address_matcher: AddressMatcher = Depends(get_address_matcher)
):
    """
    Import permit data from a file.
    """
    try:
        file_path = request.file_path
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        # Initialize permit parser
        parser = PermitParser(address_matcher=address_matcher)
        
        # Parse the file
        df = parser.parse_file(file_path, sheet_name=request.sheet_name)
        
        # Count validation errors
        error_count = 0
        if 'validation_errors' in df.columns:
            error_count = df['validation_errors'].apply(lambda x: x != '' and not pd.isna(x)).sum()
        
        # Save to output file
        output_path = f"processed_permits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_path, index=False)
        
        # Prepare sample record
        sample_record = None
        if len(df) > 0:
            sample_record = df.iloc[0].to_dict()
            
            # Remove large fields from sample
            if 'validation_errors' in sample_record:
                del sample_record['validation_errors']
        
        return ImportResult(
            status="success",
            message=f"Successfully imported {len(df)} permit records",
            record_count=len(df),
            valid_count=len(df) - error_count,
            error_count=error_count,
            sample_record=sample_record,
            output_path=output_path
        )
    
    except Exception as e:
        logger.error(f"Error importing permits: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/import/property", response_model=ImportResult, tags=["Import"])
async def import_personal_property(
    request: ImportRequest,
    background_tasks: BackgroundTasks,
    address_matcher: AddressMatcher = Depends(get_address_matcher)
):
    """
    Import personal property data from a file.
    """
    try:
        file_path = request.file_path
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
        
        # Initialize personal property parser
        parser = PersonalPropertyParser(address_matcher=address_matcher)
        
        # Parse the file
        df = parser.parse_file(file_path, sheet_name=request.sheet_name)
        
        # Count validation errors
        error_count = 0
        if 'validation_errors' in df.columns:
            error_count = df['validation_errors'].apply(lambda x: x != '' and not pd.isna(x)).sum()
        
        # Save to output file
        output_path = f"processed_property_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_path, index=False)
        
        # Prepare sample record
        sample_record = None
        if len(df) > 0:
            sample_record = df.iloc[0].to_dict()
            
            # Remove large fields from sample
            if 'validation_errors' in sample_record:
                del sample_record['validation_errors']
        
        return ImportResult(
            status="success",
            message=f"Successfully imported {len(df)} personal property records",
            record_count=len(df),
            valid_count=len(df) - error_count,
            error_count=error_count,
            sample_record=sample_record,
            output_path=output_path
        )
    
    except Exception as e:
        logger.error(f"Error importing personal property: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/lookup/parcel", response_model=List[ParcelInfo], tags=["Lookup"])
async def lookup_parcel(
    request: ParcelLookupRequest,
    db_connector: Optional[PACSConnector] = Depends(get_db_connector)
):
    """
    Look up a parcel by address.
    """
    try:
        if not db_connector:
            raise HTTPException(status_code=500, detail="Database connection not available")
        
        # Get matching parcels
        parcels = db_connector.get_parcel_by_address(request.address)
        
        if not parcels:
            return []
        
        # Convert to response format
        result = []
        for parcel in parcels:
            result.append(ParcelInfo(
                parcel_id=parcel.get('pid', ''),
                parcel_address=parcel.get('full_address', ''),
                confidence=None,  # No confidence score from direct DB lookup
                input_address=request.address
            ))
        
        return result
    
    except Exception as e:
        logger.error(f"Error looking up parcel: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/match/address", response_model=List[ParcelInfo], tags=["Lookup"])
async def match_address(
    request: ParcelLookupRequest,
    address_matcher: AddressMatcher = Depends(get_address_matcher)
):
    """
    Match an address to parcels using fuzzy matching.
    """
    try:
        if not address_matcher:
            raise HTTPException(status_code=500, detail="Address matcher not available")
        
        # Get matching parcels
        matches = address_matcher.match_address_to_parcel(
            request.address,
            min_confidence=request.min_confidence
        )
        
        if not matches:
            return []
        
        # Convert to response format
        result = []
        for match in matches:
            result.append(ParcelInfo(
                parcel_id=match.get('parcel_id', ''),
                parcel_address=match.get('parcel_address', ''),
                confidence=match.get('confidence'),
                input_address=match.get('input_address')
            ))
        
        return result
    
    except Exception as e:
        logger.error(f"Error matching address: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/config/{section}", tags=["Configuration"])
async def get_config_section(
    section: str
):
    """
    Get a configuration section.
    """
    try:
        config = ConfigManager()
        section_data = config.get(section)
        
        if section_data is None:
            raise HTTPException(status_code=404, detail=f"Configuration section not found: {section}")
        
        return JSONResponse(content=section_data)
    
    except Exception as e:
        logger.error(f"Error getting configuration section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/config", tags=["Configuration"])
async def set_config_item(
    item: ConfigItem
):
    """
    Set a configuration item.
    """
    try:
        config = ConfigManager()
        result = config.set(item.section, item.key, item.value)
        
        if not result:
            raise HTTPException(status_code=500, detail=f"Failed to set configuration item: {item.section}.{item.key}")
        
        # Save the configuration
        config.save_config()
        
        return {"status": "success", "message": f"Configuration item {item.section}.{item.key} set successfully"}
    
    except Exception as e:
        logger.error(f"Error setting configuration item: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/version", tags=["General"])
async def get_version():
    """
    Get the API version.
    """
    return {"version": __version__}


# Run the application with uvicorn
def start_api(host="127.0.0.1", port=8000):
    """
    Start the FastAPI application using uvicorn.
    """
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    start_api()
