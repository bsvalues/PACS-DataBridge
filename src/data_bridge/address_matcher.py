import re
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
from pathlib import Path

from thefuzz import fuzz, process
import pandas as pd

from .db_connector import DatabaseConnector

# Configure logging
logger = logging.getLogger(__name__)

class AddressMatcher:
    """
    Address matching service for parcel identification.
    Uses fuzzy matching to identify parcels based on address strings.
    """

    def __init__(self, db_connector: Optional[DatabaseConnector] = None):
        """
        Initialize address matcher.

        Args:
            db_connector: Optional DatabaseConnector for parcel lookup
        """
        self.db_connector = db_connector
        self.address_cache = {}
        self.threshold = 70.0  # Default threshold for fuzzy matching (0-100)

    def match_address(
        self,
        address: str,
        min_confidence: float = 70.0,
        max_results: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Match an address to parcels.

        Args:
            address: Address string to match
            min_confidence: Minimum confidence threshold (0-100)
            max_results: Maximum number of results to return

        Returns:
            List of matching parcels with confidence scores
        """
        if not address or not address.strip():
            return []

        # Normalize address
        normalized_address = self._normalize_address(address)

        # Check cache first
        if normalized_address in self.address_cache:
            logger.debug(f"Address cache hit for {normalized_address}")
            return self.address_cache[normalized_address]

        # Look up in database if available
        if self.db_connector:
            # Query database for parcels
            parcel_matches = self._lookup_parcels_by_address(normalized_address, min_confidence, max_results)

            # Cache the results
            self.address_cache[normalized_address] = parcel_matches

            return parcel_matches
        else:
            logger.warning("No database connector available for address matching")
            return []

    def _normalize_address(self, address: str) -> str:
        """
        Normalize address for consistent matching.

        Args:
            address: Raw address string

        Returns:
            Normalized address string
        """
        if not address:
            return ""

        # Convert to uppercase
        address = address.upper()

        # Remove extra whitespace
        address = re.sub(r'\s+', ' ', address.strip())

        # Replace common abbreviations
        abbrev_map = {
            r'\bAVENUE\b': 'AVE',
            r'\bAVE\b': 'AVE',
            r'\bBOULEVARD\b': 'BLVD',
            r'\bBLVD\b': 'BLVD',
            r'\bCIRCLE\b': 'CIR',
            r'\bCIR\b': 'CIR',
            r'\bCOURT\b': 'CT',
            r'\bCT\b': 'CT',
            r'\bDRIVE\b': 'DR',
            r'\bDR\b': 'DR',
            r'\bEXPRESSWAY\b': 'EXPY',
            r'\bEXPY\b': 'EXPY',
            r'\bHIGHWAY\b': 'HWY',
            r'\bHWY\b': 'HWY',
            r'\bLANE\b': 'LN',
            r'\bLN\b': 'LN',
            r'\bPARKWAY\b': 'PKWY',
            r'\bPKWY\b': 'PKWY',
            r'\bPLACE\b': 'PL',
            r'\bPL\b': 'PL',
            r'\bROAD\b': 'RD',
            r'\bRD\b': 'RD',
            r'\bSQUARE\b': 'SQ',
            r'\bSQ\b': 'SQ',
            r'\bSTREET\b': 'ST',
            r'\bST\b': 'ST',
            r'\bTERRACE\b': 'TER',
            r'\bTER\b': 'TER',
            r'\bTRAIL\b': 'TRL',
            r'\bTRL\b': 'TRL',
            r'\bWAY\b': 'WAY',

            # Directionals
            r'\bNORTH\b': 'N',
            r'\bSOUTH\b': 'S',
            r'\bEAST\b': 'E',
            r'\bWEST\b': 'W',
            r'\bNORTHEAST\b': 'NE',
            r'\bNORTHWEST\b': 'NW',
            r'\bSOUTHEAST\b': 'SE',
            r'\bSOUTHWEST\b': 'SW'
        }

        for pattern, replacement in abbrev_map.items():
            address = re.sub(pattern, replacement, address)

        # Remove common noise tokens
        noise_patterns = [
            r'\bUNIT\s+\w+\b',
            r'\bAPT\s+\w+\b',
            r'\bBUILDING\s+\w+\b',
            r'\bFLOOR\s+\w+\b',
            r'#\w+',
            r',.*'  # Remove everything after a comma
        ]

        for pattern in noise_patterns:
            address = re.sub(pattern, '', address)

        # Remove special characters
        address = re.sub(r'[^\w\s]', '', address)

        # Remove extra whitespace again
        address = re.sub(r'\s+', ' ', address.strip())

        return address

    def _lookup_parcels_by_address(
        self,
        address: str,
        min_confidence: float,
        max_results: int
    ) -> List[Dict[str, Any]]:
        """
        Look up parcels by address in the database.

        Args:
            address: Normalized address string
            min_confidence: Minimum confidence threshold
            max_results: Maximum number of results

        Returns:
            List of matching parcels with confidence scores
        """
        try:
            if not self.db_connector:
                return []

            # Extract components for more targeted search
            address_parts = self._parse_address(address)
            street_number = address_parts.get('street_number', '')
            street_name = address_parts.get('street_name', '')

            # Query database for potential matches
            # This query will depend on the specific database schema
            query = """
            SELECT
                pid,
                situs_address AS full_address,
                street_number,
                street_name,
                street_type,
                city,
                state,
                zip_code
            FROM
                parcel
            WHERE
                1=1
            """

            params = []

            # Add filters if we have specific components
            if street_number:
                query += " AND street_number = ?"
                params.append(street_number)

            if street_name:
                # Use LIKE for partial matching of street name
                query += " AND street_name LIKE ?"
                params.append(f"%{street_name}%")

            # Limit results for performance
            query += " ORDER BY pid LIMIT 100"

            # Execute query
            results = self.db_connector.execute_query(query, tuple(params))

            if not results:
                logger.debug(f"No database matches found for address: {address}")
                return []

            # Convert to list of dictionaries
            parcels = []
            for row in results:
                parcel = {
                    'pid': row[0],
                    'full_address': row[1],
                    'street_number': row[2],
                    'street_name': row[3],
                    'street_type': row[4],
                    'city': row[5],
                    'state': row[6],
                    'zip_code': row[7]
                }
                parcels.append(parcel)

            # Perform fuzzy matching
            matches = []
            for parcel in parcels:
                # Normalize database address for comparison
                db_address = self._normalize_address(parcel['full_address'])

                # Calculate fuzzy match score
                score = fuzz.token_sort_ratio(address, db_address)

                if score >= min_confidence:
                    match = parcel.copy()
                    match['confidence'] = score
                    matches.append(match)

            # Sort by confidence (descending)
            matches = sorted(matches, key=lambda x: x['confidence'], reverse=True)

            # Limit results
            matches = matches[:max_results]

            logger.debug(f"Found {len(matches)} matches for address: {address}")
            return matches

        except Exception as e:
            logger.error(f"Error looking up parcels by address: {str(e)}")
            return []

    def _parse_address(self, address: str) -> Dict[str, str]:
        """
        Parse address into components.

        Args:
            address: Normalized address string

        Returns:
            Dictionary of address components
        """
        components = {
            'street_number': '',
            'street_name': '',
            'street_type': '',
            'city': '',
            'state': '',
            'zip_code': ''
        }

        # Simple parsing - extract street number and name
        match = re.match(r'^(\d+)\s+(.+)$', address)
        if match:
            components['street_number'] = match.group(1)
            components['street_name'] = match.group(2)

            # Try to extract street type
            name_parts = components['street_name'].split()
            if len(name_parts) > 1 and name_parts[-1] in ['AVE', 'BLVD', 'CIR', 'CT', 'DR', 'EXPY', 'HWY', 'LN', 'PKWY', 'PL', 'RD', 'SQ', 'ST', 'TER', 'TRL', 'WAY']:
                components['street_type'] = name_parts[-1]
                components['street_name'] = ' '.join(name_parts[:-1])

        return components

    def clear_cache(self) -> None:
        """Clear the address cache."""
        self.address_cache = {}
        logger.debug("Address cache cleared")

    def set_threshold(self, threshold: float) -> None:
        """
        Set the minimum confidence threshold.

        Args:
            threshold: Threshold value (0-100)
        """
        if 0 <= threshold <= 100:
            self.threshold = threshold
        else:
            logger.warning(f"Invalid threshold value: {threshold}. Must be between 0 and 100.")