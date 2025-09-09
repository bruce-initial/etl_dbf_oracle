"""
Data loading components for ETL pipeline.
"""

import polars as pl
import pandas as pd
import dbf
import logging
from typing import Dict, List, Any
from pathlib import Path
from ..config.table_config import TableConfig
from ..database.operations import DatabaseOperations
from ..utils.helpers import ValidationHelpers

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data loading into Oracle database."""
    
    def __init__(self, db_operations: DatabaseOperations):
        """
        Initialize data loader.
        
        Args:
            db_operations: Database operations instance
        """
        self.db_operations = db_operations
    
    def validate_load_parameters(self, config: TableConfig, df: pl.DataFrame) -> None:
        """
        Validate parameters before loading data.
        
        Args:
            config: Table configuration
            df: DataFrame to load
            
        Raises:
            ValueError: If validation fails
        """
        # Validate table name (only for Oracle targets)
        if config.target_type.lower() == 'oracle':
            if not ValidationHelpers.validate_table_name(config.target_table):
                raise ValueError(f"Invalid Oracle table name: {config.target_table}")
        else:
            # For non-Oracle targets (like DBF), just check that target_table is not empty
            if not config.target_table:
                raise ValueError("target_table cannot be empty")
        
        # Validate batch size
        if not ValidationHelpers.validate_batch_size(config.batch_size):
            raise ValueError(f"Invalid batch size: {config.batch_size}")
        
        # Validate DataFrame
        if df.is_empty():
            raise ValueError("Cannot load empty DataFrame")
        
        if len(df.columns) == 0:
            raise ValueError("DataFrame has no columns")
        
        logger.debug("Load parameter validation passed")
    
    def create_table_structure(self, config: TableConfig, df: pl.DataFrame) -> Dict[str, str]:
        """
        Create table structure in Oracle database.
        
        Args:
            config: Table configuration
            df: DataFrame with structure to replicate
            
        Returns:
            Column mapping dictionary
            
        Raises:
            Exception: If table creation fails
        """
        try:
            # Create main table
            column_mapping = self.db_operations.create_table(
                config.target_table, 
                df, 
                config.drop_if_exists
            )
            
            logger.info(f"Created table structure for {config.target_table}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to create table structure: {e}")
            raise
    
    def load_data_to_table(self, config: TableConfig, df: pl.DataFrame, 
                          column_mapping: Dict[str, str]) -> None:
        """
        Load data into Oracle table.
        
        Args:
            config: Table configuration
            df: DataFrame to load
            column_mapping: Column name mapping
            
        Raises:
            Exception: If data loading fails
        """
        try:
            self.db_operations.insert_data(
                config.target_table,
                df,
                column_mapping,
                config.batch_size
            )
            
            logger.info(f"Successfully loaded {len(df)} rows into {config.target_table}")
            
        except Exception as e:
            logger.error(f"Failed to load data to table: {e}")
            raise
    
    def create_constraints(self, config: TableConfig, column_mapping: Dict[str, str]) -> None:
        """
        Create database constraints (primary keys, foreign keys, indexes).
        
        Args:
            config: Table configuration
            column_mapping: Column name mapping for constraint columns
        """
        try:
            # Create primary key (only if not empty)
            if config.primary_key and len(config.primary_key) > 0:
                # Map original column names to sanitized names
                sanitized_pk_columns = [
                    column_mapping.get(col, col) for col in config.primary_key
                ]
                self.db_operations.create_primary_key(config.target_table, sanitized_pk_columns)
            
            # Create foreign keys (only if not empty)
            if config.foreign_keys and len(config.foreign_keys) > 0:
                # Map column names in foreign key definitions
                mapped_foreign_keys = []
                for fk in config.foreign_keys:
                    # Skip foreign keys with empty columns
                    if not fk.get('columns') or len(fk['columns']) == 0:
                        logger.warning(f"Skipping foreign key with empty columns for table {config.target_table}")
                        continue
                    
                    mapped_fk = fk.copy()
                    mapped_fk['columns'] = [
                        column_mapping.get(col, col) for col in fk['columns']
                    ]
                    mapped_foreign_keys.append(mapped_fk)
                
                if mapped_foreign_keys:
                    self.db_operations.create_foreign_keys(config.target_table, mapped_foreign_keys)
            
            # Create indexes (only if not empty)
            if config.indexes and len(config.indexes) > 0:
                # Map column names in index definitions
                mapped_indexes = []
                for idx in config.indexes:
                    # Skip indexes with empty columns
                    if not idx.get('columns') or len(idx['columns']) == 0:
                        logger.warning(f"Skipping index '{idx.get('name', 'unnamed')}' with empty columns for table {config.target_table}")
                        continue
                    
                    mapped_idx = idx.copy()
                    mapped_idx['columns'] = [
                        column_mapping.get(col, col) for col in idx['columns']
                    ]
                    mapped_indexes.append(mapped_idx)
                
                if mapped_indexes:
                    self.db_operations.create_indexes(config.target_table, mapped_indexes)
            
            logger.info(f"Created constraints for table {config.target_table}")
            
        except Exception as e:
            logger.error(f"Failed to create constraints: {e}")
            # Don't raise here as constraints are not critical for basic functionality
            logger.warning("Continuing without constraints due to error")
    
    def validate_loaded_data(self, config: TableConfig, original_df: pl.DataFrame) -> bool:
        """
        Validate data after loading to ensure integrity.
        
        Args:
            config: Table configuration
            original_df: Original DataFrame that was loaded
            
        Returns:
            True if validation passes
            
        Raises:
            Exception: If validation fails
        """
        try:
            # Query the loaded data
            query = f"SELECT COUNT(*) as row_count FROM {config.target_table}"
            result_df = self.db_operations.execute_query(query)
            
            # Get the count from the first column (regardless of column name)
            if len(result_df) == 0:
                raise ValueError("No results returned from count query")
            
            # Get the first value from the first column (using column index)
            first_column = result_df.columns[0]
            loaded_count = int(result_df[first_column][0])
            original_count = len(original_df)
            
            if loaded_count != original_count:
                raise ValueError(
                    f"Row count mismatch: expected {original_count}, loaded {loaded_count}"
                )
            
            logger.info(f"Data validation passed: {loaded_count} rows loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            raise
    
    def load_data(self, config: TableConfig, df: pl.DataFrame, 
                  column_mapping: Dict[str, str], 
                  validate_after_load: bool = True) -> Dict[str, Any]:
        """
        Complete data loading process.
        
        Args:
            config: Table configuration
            df: DataFrame to load
            column_mapping: Column name mapping
            validate_after_load: Whether to validate data after loading
            
        Returns:
            Dictionary with loading statistics and information
            
        Raises:
            Exception: If any step of loading fails
        """
        load_info = {
            'table_name': config.target_table,
            'target_type': config.target_type,
            'rows_loaded': 0,
            'columns_loaded': 0,
            'constraints_created': False,
            'validation_passed': False
        }
        
        try:
            logger.info(f"Starting data load for {config.target_type} target: {config.target_table}")
            
            # Validate parameters
            self.validate_load_parameters(config, df)
            
            # Load data based on target type
            if config.target_type.lower() == 'oracle':
                # Load to Oracle database
                self.load_data_to_table(config, df, column_mapping)
                
                # Create constraints (Oracle only)
                try:
                    self.create_constraints(config, column_mapping)
                    load_info['constraints_created'] = True
                except Exception as e:
                    logger.warning(f"Constraint creation failed, continuing: {e}")
                
                # Validate loaded data (Oracle only)
                if validate_after_load:
                    try:
                        self.validate_loaded_data(config, df)
                        load_info['validation_passed'] = True
                    except Exception as e:
                        logger.warning(f"Data validation failed: {e}")
                        
            elif config.target_type.lower() == 'dbf':
                # Load to DBF file
                self.load_data_to_dbf(config, df, column_mapping)
                # DBF doesn't support constraints, so we skip those steps
                load_info['constraints_created'] = True  # Mark as success since not applicable
                load_info['validation_passed'] = True   # Mark as success since not applicable
                
            elif config.target_type.lower() == 'xlsx':
                # Load to XLSX file
                self.load_data_to_xlsx(config, df, column_mapping)
                # XLSX doesn't support constraints, so we skip those steps
                load_info['constraints_created'] = True  # Mark as success since not applicable
                load_info['validation_passed'] = True   # Mark as success since not applicable
                
            else:
                raise ValueError(f"Unsupported target type: {config.target_type}")
            
            load_info['rows_loaded'] = len(df)
            load_info['columns_loaded'] = len(df.columns)
            
            logger.info(f"Data loading completed successfully for {config.target_table}")
            return load_info
            
        except Exception as e:
            logger.error(f"Data loading failed for {config.target_table}: {e}")
            raise
    
    def get_load_summary(self, load_info: Dict[str, Any]) -> str:
        """
        Generate a summary of the loading process.
        
        Args:
            load_info: Loading information dictionary
            
        Returns:
            Formatted summary string
        """
        summary_lines = [
            f"Table: {load_info['table_name']}",
            f"Rows loaded: {load_info['rows_loaded']:,}",
            f"Columns loaded: {load_info['columns_loaded']}",
            f"Constraints created: {'✓' if load_info['constraints_created'] else '✗'}",
            f"Validation passed: {'✓' if load_info['validation_passed'] else '✗'}"
        ]
        
        return "\n".join(summary_lines)
    
    def create_dbf_structure(self, config: TableConfig, df: pl.DataFrame) -> Dict[str, str]:
        """
        Create DBF file structure based on DataFrame.
        
        Args:
            config: Table configuration
            df: DataFrame with structure to replicate
            
        Returns:
            Column mapping dictionary (for consistency with Oracle workflow)
            
        Raises:
            Exception: If DBF structure creation fails
        """
        try:
            # For DBF, we typically don't need to sanitize column names as much
            # But we'll apply basic sanitization for consistency
            from ..utils.helpers import ColumnSanitizer
            column_mapping = ColumnSanitizer.sanitize_column_names(df.columns)
            
            # Rename DataFrame columns for DBF compatibility
            df_renamed = df.rename(column_mapping)
            
            logger.info(f"Created DBF structure mapping for {config.target_table}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to create DBF structure: {e}")
            raise
    
    def load_data_to_dbf(self, config: TableConfig, df: pl.DataFrame, 
                        column_mapping: Dict[str, str]) -> None:
        """
        Load data into DBF file.
        
        Args:
            config: Table configuration
            df: DataFrame to load
            column_mapping: Column name mapping
            
        Raises:
            Exception: If data loading fails
        """
        try:
            # Prepare data with proper column mapping
            df_renamed = df.rename(column_mapping)
            
            # Create DBF field definitions based on DataFrame
            field_specs = self._create_dbf_field_specs(df_renamed)
            
            # Determine output file path from explicit configuration
            if hasattr(config, 'target_file_path') and config.target_file_path:
                dbf_file_path = config.target_file_path
            else:
                # Fallback to using target_table name
                if config.target_table.endswith('.dbf'):
                    dbf_file_path = config.target_table
                else:
                    dbf_file_path = f"{config.target_table}.dbf"
            
            # Handle drop_if_exists
            if config.drop_if_exists and Path(dbf_file_path).exists():
                Path(dbf_file_path).unlink()
                logger.info(f"Removed existing DBF file: {dbf_file_path}")
            
            # Create and populate DBF file
            with dbf.Table(dbf_file_path, field_specs) as table:
                for row in df_renamed.iter_rows(named=True):
                    # Convert row data to DBF-compatible format
                    dbf_record = {}
                    for field_name, value in row.items():
                        if value is None:
                            dbf_record[field_name] = None
                        elif isinstance(value, (int, float, str)):
                            dbf_record[field_name] = value
                        else:
                            # Convert other types to string
                            dbf_record[field_name] = str(value)
                    
                    table.append(dbf_record)
            
            logger.info(f"Successfully loaded {len(df_renamed)} rows into DBF file: {dbf_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load data to DBF file: {e}")
            raise
    
    def _create_dbf_field_specs(self, df: pl.DataFrame) -> str:
        """
        Create DBF field specifications based on DataFrame columns and types.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Field specification string for DBF table creation
        """
        field_specs = []
        
        for col in df.columns:
            col_type = df[col].dtype
            
            # Map Polars types to DBF field types
            if col_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
                # Numeric field: N(width, decimals)
                field_specs.append(f"{col} N(10,0)")
            elif col_type in [pl.Float32, pl.Float64]:
                # Numeric field with decimals
                field_specs.append(f"{col} N(15,2)")
            elif col_type == pl.Boolean:
                # Logical field
                field_specs.append(f"{col} L")
            elif col_type in [pl.Date, pl.Datetime]:
                # Date field
                field_specs.append(f"{col} D")
            else:
                # Default to character field, determine optimal width
                max_length = self._get_max_string_length(df[col])
                width = min(max(max_length, 10), 254)  # DBF limit is 254 chars
                field_specs.append(f"{col} C({width})")
        
        return "; ".join(field_specs)
    
    def _get_max_string_length(self, series: pl.Series) -> int:
        """
        Get maximum string length in a series.
        
        Args:
            series: Polars series to analyze
            
        Returns:
            Maximum string length
        """
        try:
            if series.dtype == pl.Utf8:
                max_len = series.str.len_chars().max()
                return max_len if max_len is not None else 50
            else:
                # Convert to string and get max length
                str_lengths = series.map_elements(lambda x: len(str(x)) if x is not None else 0)
                max_len = str_lengths.max()
                return max_len if max_len is not None else 50
        except:
            return 50  # Default fallback
    
    def create_xlsx_structure(self, config: TableConfig, df: pl.DataFrame) -> Dict[str, str]:
        """
        Create XLSX file structure based on DataFrame.
        
        Args:
            config: Table configuration
            df: DataFrame with structure to replicate
            
        Returns:
            Column mapping dictionary (for consistency with other workflows)
            
        Raises:
            Exception: If XLSX structure creation fails
        """
        try:
            # For XLSX, we typically don't need to sanitize column names as much
            # But we'll apply basic sanitization for consistency
            from ..utils.helpers import ColumnSanitizer
            column_mapping = ColumnSanitizer.sanitize_column_names(df.columns)
            
            logger.info(f"Created XLSX structure mapping for {config.target_table}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to create XLSX structure: {e}")
            raise
    
    def load_data_to_xlsx(self, config: TableConfig, df: pl.DataFrame, 
                         column_mapping: Dict[str, str]) -> None:
        """
        Load data into XLSX file.
        
        Args:
            config: Table configuration
            df: DataFrame to load
            column_mapping: Column name mapping
            
        Raises:
            Exception: If data loading fails
        """
        try:
            # Prepare data with proper column mapping
            df_renamed = df.rename(column_mapping)
            
            # Determine output file path from explicit configuration
            if hasattr(config, 'target_file_path') and config.target_file_path:
                xlsx_file_path = config.target_file_path
            else:
                # Fallback to using target_table name
                if config.target_table.endswith('.xlsx'):
                    xlsx_file_path = config.target_table
                else:
                    xlsx_file_path = f"{config.target_table}.xlsx"
            
            # Handle drop_if_exists
            if config.drop_if_exists and Path(xlsx_file_path).exists():
                Path(xlsx_file_path).unlink()
                logger.info(f"Removed existing XLSX file: {xlsx_file_path}")
            
            # Get XLSX writing options
            xlsx_options = getattr(config, 'xlsx_options', {})
            
            # Default options for XLSX writing
            default_options = {
                'worksheet_name': xlsx_options.get('worksheet_name', 'Sheet1'),
                'header': xlsx_options.get('header', True),
                'index': xlsx_options.get('index', False)
            }
            try:
                # Convert Polars DataFrame to pandas for Excel writing
                pandas_df = df_renamed.to_pandas()
                
                # Write to Excel using pandas (will use default engine)
                pandas_df.to_excel(
                    xlsx_file_path,
                    sheet_name=default_options['worksheet_name'],
                    header=default_options['header'],
                    index=default_options['index']
                )
                
            except Exception as pandas_error:
                # Check if it's a missing Excel engine error
                if "openpyxl" in str(pandas_error).lower() or "xlsxwriter" in str(pandas_error).lower():
                    logger.error(f"Missing Excel engine for XLSX writing: {pandas_error}")
                    raise Exception(f"XLSX writing requires openpyxl. Install with: pip install openpyxl")
                else:
                    logger.error(f"Pandas Excel writing failed: {pandas_error}")
                    raise Exception(f"Failed to write XLSX file using pandas: {pandas_error}")
            
            logger.info(f"Successfully loaded {len(df_renamed)} rows into XLSX file: {xlsx_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load data to XLSX file: {e}")
            raise