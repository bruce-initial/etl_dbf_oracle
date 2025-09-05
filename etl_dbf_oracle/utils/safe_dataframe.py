"""
Safe DataFrame creation utilities to prevent schema inference errors.
Provides wrappers that ensure all data is properly homogenized before DataFrame creation.
"""

import polars as pl
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)


class SafeDataFrameCreator:
    """
    Safe wrapper for Polars DataFrame creation to prevent schema inference errors.
    Ensures all data is properly homogenized as strings before DataFrame creation.
    """
    
    @staticmethod
    def create_dataframe(data: Union[Dict[str, List], List[Dict]], 
                        schema: Optional[Dict[str, pl.DataType]] = None,
                        force_string_schema: bool = True) -> pl.DataFrame:
        """
        Safely create a Polars DataFrame with proper type handling.
        
        Args:
            data: Dictionary of column data or list of row dictionaries
            schema: Optional schema specification
            force_string_schema: If True, forces all columns to be strings
            
        Returns:
            Polars DataFrame with consistent schema
        """
        try:
            if force_string_schema:
                logger.debug("Creating DataFrame with forced string schema")
                
                # Convert data to homogeneous string format
                if isinstance(data, dict):
                    # Dictionary format: {col1: [val1, val2], col2: [val3, val4]}
                    string_data = {}
                    for col, values in data.items():
                        string_data[col] = [str(v) if v is not None else None for v in values]
                    
                    # Create string schema
                    string_schema = {col: pl.Utf8 for col in string_data.keys()}
                    return pl.DataFrame(string_data, schema=string_schema)
                    
                elif isinstance(data, list):
                    # List of dictionaries format: [{'col1': val1, 'col2': val2}, ...]
                    if not data:
                        return pl.DataFrame({})
                    
                    # Get all unique column names
                    all_cols = set()
                    for row in data:
                        if isinstance(row, dict):
                            all_cols.update(row.keys())
                    
                    # Convert to dict format with string values
                    string_data = {}
                    for col in all_cols:
                        string_data[col] = []
                        for row in data:
                            val = row.get(col)
                            string_data[col].append(str(val) if val is not None else None)
                    
                    # Create string schema
                    string_schema = {col: pl.Utf8 for col in string_data.keys()}
                    return pl.DataFrame(string_data, schema=string_schema)
                    
                else:
                    logger.warning(f"Unsupported data type for safe DataFrame creation: {type(data)}")
                    return pl.DataFrame(data, schema=schema)
            else:
                # Use provided schema or let Polars infer
                return pl.DataFrame(data, schema=schema)
                
        except Exception as e:
            logger.error(f"Safe DataFrame creation failed: {e}")
            
            # Ultimate fallback: Try to create with minimal data
            try:
                if isinstance(data, dict):
                    # Create empty DataFrame with string columns
                    empty_string_data = {col: [] for col in data.keys()}
                    string_schema = {col: pl.Utf8 for col in data.keys()}
                    return pl.DataFrame(empty_string_data, schema=string_schema)
                else:
                    return pl.DataFrame({})
            except Exception as fallback_error:
                logger.error(f"Even fallback DataFrame creation failed: {fallback_error}")
                raise fallback_error
    
    @staticmethod
    def safe_from_pandas(pandas_df: pd.DataFrame, 
                        force_string_schema: bool = True,
                        schema_overrides: Optional[Dict[str, pl.DataType]] = None) -> pl.DataFrame:
        """
        Safely convert pandas DataFrame to Polars DataFrame.
        
        Args:
            pandas_df: Pandas DataFrame to convert
            force_string_schema: If True, converts all columns to strings first
            schema_overrides: Optional schema overrides
            
        Returns:
            Polars DataFrame with consistent schema
        """
        try:
            if force_string_schema:
                logger.debug("Converting pandas DataFrame with forced string schema")
                
                # Create a copy to avoid modifying original
                safe_df = pandas_df.copy()
                
                # Convert all columns to strings
                for col in safe_df.columns:
                    try:
                        # Convert to string, handling various types
                        safe_df[col] = safe_df[col].astype(str)
                        # Clean up string representations of None/NaN
                        safe_df[col] = safe_df[col].replace({
                            'nan': None, 'NaN': None, '<NA>': None, 
                            'None': None, 'null': None, 'NULL': None
                        })
                    except Exception as col_error:
                        logger.warning(f"Failed to convert column '{col}' to string: {col_error}")
                
                # Create string schema override
                string_schema = {col: pl.Utf8 for col in safe_df.columns}
                
                # Convert to Polars with explicit string schema
                return pl.from_pandas(safe_df, schema_overrides=string_schema)
                
            else:
                # Use provided schema overrides or default conversion
                return pl.from_pandas(pandas_df, schema_overrides=schema_overrides)
                
        except Exception as e:
            logger.error(f"Safe pandas to Polars conversion failed: {e}")
            
            # Fallback: Try with empty DataFrame with same columns
            try:
                empty_data = {col: [] for col in pandas_df.columns}
                string_schema = {col: pl.Utf8 for col in pandas_df.columns}
                return pl.DataFrame(empty_data, schema=string_schema)
            except Exception as fallback_error:
                logger.error(f"Pandas conversion fallback failed: {fallback_error}")
                raise fallback_error
    
    @staticmethod
    def homogenize_records_for_dataframe(records: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Convert list of record dictionaries to homogeneous column data for DataFrame creation.
        
        Args:
            records: List of dictionaries representing rows
            
        Returns:
            Dictionary with column names as keys and homogeneous lists as values
        """
        if not records:
            return {}
        
        logger.debug(f"Homogenizing {len(records)} records for DataFrame creation")
        
        # Get all unique column names
        all_columns = set()
        for record in records:
            if isinstance(record, dict):
                all_columns.update(record.keys())
        
        logger.debug(f"Found {len(all_columns)} unique columns: {list(all_columns)[:10]}{'...' if len(all_columns) > 10 else ''}")
        
        # Create homogeneous data structure
        column_data = {}
        for col in all_columns:
            column_values = []
            value_types = set()  # Track value types for debugging
            
            for record_idx, record in enumerate(records):
                value = record.get(col)
                
                # Convert everything to strings to ensure homogeneity
                if value is None:
                    column_values.append(None)
                    value_types.add(type(None))
                else:
                    try:
                        # Extra aggressive string conversion
                        if isinstance(value, str):
                            # Already a string, just clean it
                            clean_value = str(value).strip()
                            # Handle the problematic "2005-01-07" case specifically
                            if clean_value and len(clean_value) == 10 and clean_value.count('-') == 2:
                                # This looks like a date string - ensure it's treated as a string
                                column_values.append(clean_value)
                            else:
                                column_values.append(clean_value if clean_value else None)
                        else:
                            # Convert non-string types to string
                            str_value = str(value).strip()
                            column_values.append(str_value if str_value else None)
                        
                        value_types.add(type(value))
                        
                    except Exception as e:
                        logger.warning(f"Failed to convert value to string for column '{col}' at record {record_idx}: {e}")
                        logger.warning(f"Problematic value: {repr(value)} (type: {type(value)})")
                        
                        # Ultimate fallback - try to convert to string with truncation
                        try:
                            fallback_str = str(value)[:100] 
                            column_values.append(fallback_str if fallback_str else None)
                        except Exception as fallback_error:
                            logger.error(f"Even fallback string conversion failed: {fallback_error}")
                            column_values.append(None)  # Give up and use None
            
            # Log if there were mixed types in a column
            if len(value_types) > 2:  # More than string and None
                logger.debug(f"Column '{col}' had mixed types: {value_types}")
            
            column_data[col] = column_values
        
        logger.debug(f"Homogenization complete: {len(column_data)} columns, {len(records)} rows")
        return column_data


# Convenience functions for easy import
def safe_dataframe(data, **kwargs):
    """Convenience function for safe DataFrame creation."""
    return SafeDataFrameCreator.create_dataframe(data, **kwargs)


def safe_from_pandas(pandas_df, **kwargs):
    """Convenience function for safe pandas to Polars conversion."""
    return SafeDataFrameCreator.safe_from_pandas(pandas_df, **kwargs)


def safe_records_to_dataframe(records: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Convert list of records to DataFrame with maximum safety.
    
    Args:
        records: List of dictionaries
        
    Returns:
        Polars DataFrame with string schema
    """
    if not records:
        return pl.DataFrame({})
    
    try:
        # Primary approach: Homogenize records and create DataFrame
        column_data = SafeDataFrameCreator.homogenize_records_for_dataframe(records)
        return SafeDataFrameCreator.create_dataframe(column_data, force_string_schema=True)
        
    except Exception as primary_error:
        logger.error(f"Primary safe DataFrame creation failed: {primary_error}")
        logger.info("Attempting column-by-column DataFrame creation for maximum safety")
        
        # Ultra-safe fallback: Create DataFrame column by column
        return _create_dataframe_column_by_column(records)


def _create_dataframe_column_by_column(records: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Ultimate fallback: Create DataFrame column by column to isolate problematic columns.
    
    Args:
        records: List of dictionaries
        
    Returns:
        Polars DataFrame created column by column
    """
    if not records:
        return pl.DataFrame({})
    
    # Get all unique column names
    all_columns = set()
    for record in records:
        if isinstance(record, dict):
            all_columns.update(record.keys())
    
    logger.info(f"Creating DataFrame column-by-column for {len(all_columns)} columns")
    
    # Create columns one by one
    column_series = {}
    for col_name in sorted(all_columns):  # Sort for consistent order
        try:
            # Extract all values for this column
            column_values = []
            for record in records:
                value = record.get(col_name)
                
                # Super aggressive string conversion
                if value is None:
                    column_values.append(None)
                else:
                    try:
                        # Convert to string with extra safety
                        str_value = str(value)
                        # Special handling for date-like strings
                        if str_value and len(str_value) >= 8 and '-' in str_value:
                            # Ensure date strings are treated as plain strings
                            column_values.append(str_value)
                        else:
                            column_values.append(str_value if str_value else None)
                    except Exception as conv_error:
                        logger.warning(f"Failed to convert value for column '{col_name}': {conv_error}")
                        column_values.append(None)
            
            # Create Polars Series with explicit string type
            try:
                series = pl.Series(col_name, column_values, dtype=pl.Utf8)
                column_series[col_name] = series
                logger.debug(f"Successfully created series for column '{col_name}': {len(column_values)} values")
                
            except Exception as series_error:
                logger.error(f"Failed to create series for column '{col_name}': {series_error}")
                logger.info(f"Sample values for '{col_name}': {column_values[:3]}")
                
                # Create an empty series if individual column fails
                empty_series = pl.Series(col_name, [None] * len(records), dtype=pl.Utf8)
                column_series[col_name] = empty_series
                
        except Exception as col_error:
            logger.error(f"Column '{col_name}' processing completely failed: {col_error}")
            # Create empty series for failed column
            empty_series = pl.Series(col_name, [None] * len(records), dtype=pl.Utf8)
            column_series[col_name] = empty_series
    
    # Combine all series into DataFrame
    try:
        if column_series:
            df = pl.DataFrame(column_series)
            logger.info(f"✅ Column-by-column DataFrame creation successful: {len(df)} rows, {len(df.columns)} columns")
            return df
        else:
            logger.warning("No columns could be processed, returning empty DataFrame")
            return pl.DataFrame({})
            
    except Exception as combine_error:
        logger.error(f"Failed to combine columns into DataFrame: {combine_error}")
        # Last resort: create completely empty DataFrame
        return pl.DataFrame({})