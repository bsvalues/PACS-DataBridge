"""
Address Matcher Module

Provides functionality to match and standardize addresses between permit data
and parcel records in the PACS database.
"""

import re
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
import pandas as pd
from difflib import SequenceMatcher
from dataclasses import dataclass
from fuzzywuzzy import fuzz, process

from data_bridge.db_connector import PACSConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Address:
    """Structured representation of an address."""
    street_number: str = ""
    street_name: str = ""
    street_type: str = ""
    unit_number: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    
    @property
    def full_address(self) -> str:
        """Get the full address as a formatted string."""
        street = f"{self.street_number} {self.street_name}"
        if self.street_type:
            street += f" {self.street_type}"
        
        unit = f"Unit {self.unit_number}" if self.unit_number else ""
        
        city_state_zip = ""
        if self.city:
            city_state_zip += self.city
            if self.state:
                city_state_zip += f", {self.state}"
                if self.zip_code:
                    city_state_zip += f" {self.zip_code}"
        
        parts = [part for part in [street, unit, city_state_zip] if part]
        return ", ".join(parts)

class AddressMatcher:
    """
    Address matching and standardization for the PACS DataBridge system.
    Provides functionality to parse, standardize, and match addresses.
    """
    
    # Common street type abbreviations and their standardized forms
    STREET_TYPES = {
        "ST": "STREET", "AVE": "AVENUE", "BLVD": "BOULEVARD", "DR": "DRIVE",
        "RD": "ROAD", "LN": "LANE", "CT": "COURT", "PL": "PLACE", "TER": "TERRACE",
        "CIR": "CIRCLE", "HWY": "HIGHWAY", "PKWY": "PARKWAY", "WAY": "WAY",
        "STREET": "STREET", "AVENUE": "AVENUE", "BOULEVARD": "BOULEVARD",
        "DRIVE": "DRIVE", "ROAD": "ROAD", "LANE": "LANE", "COURT": "COURT",
        "PLACE": "PLACE", "TERRACE": "TERRACE", "CIRCLE": "CIRCLE",
        "HIGHWAY": "HIGHWAY", "PARKWAY": "PARKWAY"
    }
    
    # Directional abbreviations and standardizations
    DIRECTIONS = {
        "N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST",
        "NE": "NORTHEAST", "NW": "NORTHWEST", "SE": "SOUTHEAST", "SW": "SOUTHWEST",
        "NORTH": "NORTH", "SOUTH": "SOUTH", "EAST": "EAST", "WEST": "WEST",
        "NORTHEAST": "NORTHEAST", "NORTHWEST": "NORTHWEST", 
        "SOUTHEAST": "SOUTHEAST", "SOUTHWEST": "SOUTHWEST"
    }
    
    def __init__(self, pacs_connector: Optional[PACSConnector] = None):
        """
        Initialize the AddressMatcher.
        
        Args:
            pacs_connector: Optional PACSConnector for database lookups
        """
        self.pacs_connector = pacs_connector
    
    def parse_address(self, address_str: str) -> Address:
        """
        Parse an address string into structured components.
        
        Args:
            address_str: Full address string to parse
            
        Returns:
            Structured Address object
        """
        # Initialize Address object
        address = Address()
        
        # Handle None or empty string
        if not address_str:
            return address
        
        # Clean the address string
        clean_address = address_str.strip().upper()
        
        # Extract zip code
        zip_pattern = r'(\d{5}(-\d{4})?)'
        zip_match = re.search(zip_pattern, clean_address)
        if zip_match:
            address.zip_code = zip_match.group(1)
            clean_address = clean_address.replace(zip_match.group(1), "").strip().rstrip(",")
        
        # Extract state (2-letter code)
        state_pattern = r',\s*([A-Z]{2})\s*$'
        state_match = re.search(state_pattern, clean_address)
        if state_match:
            address.state = state_match.group(1)
            clean_address = clean_address.replace(state_match.group(0), "").strip()
        
        # Extract city
        city_pattern = r',\s*([A-Za-z\s\.]+?)\s*$'
        city_match = re.search(city_pattern, clean_address)
        if city_match:
            address.city = city_match.group(1).strip()
            clean_address = clean_address.replace(city_match.group(0), "").strip()
        
        # Extract unit number
        unit_patterns = [
            r'(?:UNIT|APT|APARTMENT|#)\s*([A-Z0-9\-]+)',
            r'(?:SUITE|STE)\s*([A-Z0-9\-]+)'
        ]
        
        for pattern in unit_patterns:
            unit_match = re.search(pattern, clean_address)
            if unit_match:
                address.unit_number = unit_match.group(1)
                clean_address = clean_address.replace(unit_match.group(0), "").strip()
                break
        
        # Process street components
        parts = clean_address.split()
        
        # Extract street number
        if parts and parts[0].isdigit():
            address.street_number = parts[0]
            parts = parts[1:]
        
        # Look for street type
        street_type_found = False
        for i, part in enumerate(parts):
            if part in self.STREET_TYPES:
                address.street_type = self.STREET_TYPES[part]
                address.street_name = " ".join(parts[:i])
                street_type_found = True
                break
        
        # If no street type found, assume all remaining parts are street name
        if not street_type_found and parts:
            address.street_name = " ".join(parts)
        
        # Standardize directions in street name
        for direction, standard in self.DIRECTIONS.items():
            # Check for direction at start of street name
            if address.street_name.startswith(direction + " "):
                address.street_name = standard + address.street_name[len(direction):]
            
            # Check for direction at end of street name
            if address.street_name.endswith(" " + direction):
                address.street_name = address.street_name[:-len(direction)] + standard
        
        return address
    
    def standardize_address(self, address: Union[str, Address]) -> Address:
        """
        Standardize an address by parsing and normalizing its components.
        
        Args:
            address: Address string or Address object to standardize
            
        Returns:
            Standardized Address object
        """
        # Parse if string, otherwise use the provided Address object
        if isinstance(address, str):
            parsed_address = self.parse_address(address)
        else:
            parsed_address = address
        
        # Standardize the address components
        standardized = Address(
            street_number=parsed_address.street_number,
            street_name=parsed_address.street_name.upper(),
            street_type=parsed_address.street_type.upper() if parsed_address.street_type else "",
            unit_number=parsed_address.unit_number.upper() if parsed_address.unit_number else "",
            city=parsed_address.city.upper() if parsed_address.city else "",
            state=parsed_address.state.upper() if parsed_address.state else "",
            zip_code=parsed_address.zip_code
        )
        
        # Standardize directionals in street name
        for direction, standard in self.DIRECTIONS.items():
            direction_pattern = rf'\\b{direction}\\b'
            standardized.street_name = re.sub(
                direction_pattern, 
                standard, 
                standardized.street_name
            )
        
        return standardized
    
    def match_address_to_parcel(
        self, 
        address: Union[str, Address], 
        min_confidence: float = 70.0
    ) -> List[Dict[str, Any]]:
        """
        Match an address to parcels in the PACS database.
        
        Args:
            address: Address string or Address object to match
            min_confidence: Minimum confidence score (0-100) for matches
            
        Returns:
            List of matching parcels with confidence scores
        """
        if not self.pacs_connector:
            logger.error("No PACS connector provided for address matching")
            return []
        
        # Standardize the address
        if isinstance(address, str):
            std_address = self.standardize_address(address)
            address_str = address
        else:
            std_address = self.standardize_address(address)
            address_str = address.full_address
        
        # Get potential matches from database
        potential_parcels = self.pacs_connector.get_parcel_by_address(
            f"{std_address.street_number} {std_address.street_name}"
        )
        
        if not potential_parcels:
            return []
        
        # Score each potential match
        scored_matches = []
        for parcel in potential_parcels:
            # Create standardized address from parcel data
            parcel_address = Address(
                street_number=str(parcel.get('street_number', '')),
                street_name=parcel.get('street_name', ''),
                city=parcel.get('city', ''),
                state=parcel.get('state', ''),
                zip_code=parcel.get('zip', '')
            )
            
            # Calculate match score
            score = self._calculate_address_match_score(std_address, parcel_address)
            
            # Add to results if above threshold
            if score >= min_confidence:
                match = {
                    'parcel_id': parcel.get('pid', ''),
                    'parcel_address': parcel.get('full_address', ''),
                    'confidence': score,
                    'input_address': address_str
                }
                scored_matches.append(match)
        
        # Sort by confidence score
        scored_matches.sort(key=lambda x: x['confidence'], reverse=True)
        
        return scored_matches
    
    def _calculate_address_match_score(self, address1: Address, address2: Address) -> float:
        """
        Calculate a match score between two addresses.
        
        Args:
            address1: First address to compare
            address2: Second address to compare
            
        Returns:
            Match confidence score (0-100)
        """
        # Component weights
        weights = {
            'street_number': 40,  # Most important for matching
            'street_name': 35,
            'street_type': 5,
            'unit_number': 10,
            'city': 5,
            'state': 3,
            'zip_code': 2
        }
        
        total_weight = sum(weights.values())
        weighted_score = 0
        
        # Compare street number (exact match only)
        if address1.street_number == address2.street_number:
            weighted_score += weights['street_number']
        
        # Compare street name (fuzzy match)
        street_name_score = fuzz.token_sort_ratio(address1.street_name, address2.street_name)
        weighted_score += (street_name_score / 100) * weights['street_name']
        
        # Compare street type
        if address1.street_type == address2.street_type:
            weighted_score += weights['street_type']
        
        # Compare unit number (if both have it)
        if address1.unit_number and address2.unit_number:
            if address1.unit_number == address2.unit_number:
                weighted_score += weights['unit_number']
        elif not address1.unit_number and not address2.unit_number:
            weighted_score += weights['unit_number']  # Both have no unit number
        
        # Compare city
        if address1.city and address2.city:
            city_score = fuzz.ratio(address1.city, address2.city)
            weighted_score += (city_score / 100) * weights['city']
        
        # Compare state
        if address1.state == address2.state:
            weighted_score += weights['state']
        
        # Compare zip code
        if address1.zip_code == address2.zip_code:
            weighted_score += weights['zip_code']
        
        # Calculate final score (0-100)
        final_score = (weighted_score / total_weight) * 100
        
        return final_score


# Example usage
if __name__ == "__main__":
    # Create an AddressMatcher (without database connector for demo purposes)
    matcher = AddressMatcher()
    
    # Test address parsing
    test_address = "123 N Main St, Apt 4B, Springfield, WA 98123"
    parsed = matcher.parse_address(test_address)
    print(f"Parsed address: {parsed}")
    print(f"Full address: {parsed.full_address}")
    
    # Test address standardization
    standardized = matcher.standardize_address(test_address)
    print(f"Standardized address: {standardized}")
    print(f"Full standardized address: {standardized.full_address}")
    
    # Note: To test matching with the database, a PACSConnector would be needed
