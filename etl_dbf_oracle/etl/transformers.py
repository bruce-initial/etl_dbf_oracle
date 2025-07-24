"""
Data transformation components for ETL pipeline.
"""

import polars as pl
import logging
from typing import Dict, List, Any, Optional
from ..utils.helpers import ColumnSanitizer

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation operations."""
    
    def __init__(self):
        """Initialize data transformer."""
        self.column_sanitizer = ColumnSanitizer()
    
    def sanitize_column_names(self, df: pl.DataFrame) -> tuple[pl.DataFrame, Dict[str, str]]:
        """
        Sanitize DataFrame column names for Oracle compatibility.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (transformed DataFrame, column mapping)
        """
        # First trim whitespace from column names
        trimmed_columns = [col.strip() for col in df.columns]
        df_with_trimmed_cols = df
        if trimmed_columns != df.columns:
            column_rename_map = {old: new for old, new in zip(df.columns, trimmed_columns)}
            df_with_trimmed_cols = df.rename(column_rename_map)
            logger.info(f"Trimmed whitespace from column names: {column_rename_map}")
        
        # Get column mapping for Oracle compatibility
        column_mapping = self.column_sanitizer.sanitize_column_names(df_with_trimmed_cols.columns)
        
        # Rename columns in DataFrame
        transformed_df = df_with_trimmed_cols.rename(column_mapping)
        
        logger.info(f"Sanitized column names for {len(df.columns)} columns")
        logger.debug(f"Column mapping: {column_mapping}")
        
        return transformed_df, column_mapping
    
    def handle_null_values(self, df: pl.DataFrame, null_strategy: str = 'keep') -> pl.DataFrame:
        """
        Handle null values in DataFrame based on strategy.
        
        Args:
            df: Input DataFrame
            null_strategy: Strategy for handling nulls ('keep', 'drop_rows', 'fill_default')
            
        Returns:
            Transformed DataFrame
        """
        if null_strategy == 'keep':
            return df
        elif null_strategy == 'drop_rows':
            # Drop rows with any null values
            transformed_df = df.drop_nulls()
            logger.info(f"Dropped rows with nulls: {len(df) - len(transformed_df)} rows removed")
            return transformed_df
        elif null_strategy == 'fill_default':
            # Fill nulls with appropriate defaults based on data type
            transformations = []
            for col in df.columns:
                dtype = df[col].dtype
                if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                           pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                    transformations.append(pl.col(col).fill_null(0))
                elif dtype in [pl.Float32, pl.Float64]:
                    transformations.append(pl.col(col).fill_null(0.0))
                elif dtype == pl.Utf8:
                    transformations.append(pl.col(col).fill_null(""))
                elif dtype == pl.Boolean:
                    transformations.append(pl.col(col).fill_null(False))
                else:
                    transformations.append(pl.col(col))  # Keep as is for other types
            
            transformed_df = df.with_columns(transformations)
            logger.info("Filled null values with default values")
            return transformed_df
        else:
            raise ValueError(f"Unknown null strategy: {null_strategy}")
    
    def validate_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Validate and fix data types for Oracle compatibility.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with validated data types
        """
        transformations = []
        
        for col in df.columns:
            col_data = df[col]
            dtype = col_data.dtype
            
            # Handle problematic data types
            if dtype == pl.Object:
                # Try to infer better type for Object columns
                logger.warning(f"Column {col} has Object type, attempting to cast to string")
                transformations.append(pl.col(col).cast(pl.Utf8, strict=False))
            elif isinstance(dtype, pl.List):
                # Convert list columns to string representation
                logger.warning(f"Column {col} has List type, converting to string")
                transformations.append(pl.col(col).map_elements(str, return_dtype=pl.Utf8))
            elif isinstance(dtype, pl.Struct):
                # Convert struct columns to string representation
                logger.warning(f"Column {col} has Struct type, converting to string")
                transformations.append(pl.col(col).map_elements(str, return_dtype=pl.Utf8))
            else:
                transformations.append(pl.col(col))
        
        if len(transformations) != len(df.columns):
            transformed_df = df.with_columns(transformations)
            logger.info("Validated and fixed data types for Oracle compatibility")
            return transformed_df
        
        return df
    
    def trim_string_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Trim whitespace from string columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with trimmed string columns
        """
        transformations = []
        string_cols_trimmed = 0
        
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                transformations.append(pl.col(col).str.strip_chars())
                string_cols_trimmed += 1
            else:
                transformations.append(pl.col(col))
        
        if string_cols_trimmed > 0:
            transformed_df = df.with_columns(transformations)
            logger.info(f"Trimmed whitespace from {string_cols_trimmed} string columns")
            return transformed_df
        
        return df
    
    def apply_column_transformations(self, df: pl.DataFrame, 
                                   transformations: List[Dict[str, Any]]) -> pl.DataFrame:
        """
        Apply custom column transformations.
        
        Args:
            df: Input DataFrame
            transformations: List of transformation definitions
                           Format: [{'column': 'col_name', 'operation': 'upper', 'params': {}}]
            
        Returns:
            Transformed DataFrame
        """
        if not transformations:
            return df
        
        polars_transformations = []
        
        for transform in transformations:
            col_name = transform['column']
            operation = transform['operation']
            params = transform.get('params', {})
            
            if col_name not in df.columns:
                logger.warning(f"Column {col_name} not found in DataFrame, skipping transformation")
                continue
            
            if operation == 'upper':
                polars_transformations.append(pl.col(col_name).str.to_uppercase())
            elif operation == 'lower':
                polars_transformations.append(pl.col(col_name).str.to_lowercase())
            elif operation == 'replace':
                old_val = params.get('old', '')
                new_val = params.get('new', '')
                polars_transformations.append(pl.col(col_name).str.replace_all(old_val, new_val))
            elif operation == 'cast':
                target_type = params.get('type', pl.Utf8)
                polars_transformations.append(pl.col(col_name).cast(target_type, strict=False))
            else:
                logger.warning(f"Unknown transformation operation: {operation}")
                polars_transformations.append(pl.col(col_name))
        
        if polars_transformations:
            # Only transform specified columns, keep others as is
            other_cols = [pl.col(col) for col in df.columns 
                         if col not in [t['column'] for t in transformations]]
            all_transformations = polars_transformations + other_cols
            
            transformed_df = df.with_columns(all_transformations)
            logger.info(f"Applied {len(polars_transformations)} custom transformations")
            return transformed_df
        
        return df
    
    def remove_duplicate_rows(self, df: pl.DataFrame, subset: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df: Input DataFrame
            subset: List of columns to consider for duplicates (None for all columns)
            
        Returns:
            DataFrame with duplicates removed
        """
        original_count = len(df)
        
        if subset:
            transformed_df = df.unique(subset=subset)
        else:
            transformed_df = df.unique()
        
        duplicates_removed = original_count - len(transformed_df)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows")
        
        return transformed_df
    
    def transform_data(self, df: pl.DataFrame, 
                      apply_column_sanitization: bool = True,
                      null_strategy: str = 'keep',
                      trim_strings: bool = True,
                      validate_types: bool = True,
                      remove_duplicates: bool = False,
                      duplicate_subset: Optional[List[str]] = None,
                      custom_transformations: Optional[List[Dict[str, Any]]] = None) -> tuple[pl.DataFrame, Optional[Dict[str, str]]]:
        """
        Apply comprehensive data transformations.
        
        Args:
            df: Input DataFrame
            apply_column_sanitization: Whether to sanitize column names
            null_strategy: Strategy for handling null values
            trim_strings: Whether to trim string columns
            validate_types: Whether to validate data types
            remove_duplicates: Whether to remove duplicate rows
            duplicate_subset: Columns to consider for duplicate removal
            custom_transformations: List of custom transformation definitions
            
        Returns:
            Tuple of (transformed DataFrame, column mapping if sanitization applied)
        """
        transformed_df = df
        column_mapping = None
        
        logger.info(f"Starting data transformation for DataFrame: {df.shape}")
        
        # 1. Validate and fix data types
        if validate_types:
            transformed_df = self.validate_data_types(transformed_df)
        
        # 2. Handle null values
        if null_strategy != 'keep':
            transformed_df = self.handle_null_values(transformed_df, null_strategy)
        
        # 3. Trim string columns
        if trim_strings:
            transformed_df = self.trim_string_columns(transformed_df)
        
        # 4. Apply custom transformations
        if custom_transformations:
            transformed_df = self.apply_column_transformations(transformed_df, custom_transformations)
        
        # 5. Remove duplicates
        if remove_duplicates:
            transformed_df = self.remove_duplicate_rows(transformed_df, duplicate_subset)
        
        # 6. Sanitize column names (should be last to avoid name mismatches)
        if apply_column_sanitization:
            transformed_df, column_mapping = self.sanitize_column_names(transformed_df)
        
        logger.info(f"Data transformation completed: {transformed_df.shape}")
        return transformed_df, column_mapping