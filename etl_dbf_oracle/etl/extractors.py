"""
Data extraction components for ETL pipeline.
"""

import polars as pl
import pandas as pd
import duckdb
import dbf
import logging
from typing import Optional
from pathlib import Path
from ..config.table_config import TableConfig
from ..database.operations import DatabaseOperations
from ..utils.helpers import ValidationHelpers


logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from various sources."""
    
    def __init__(self, db_operations: Optional[DatabaseOperations] = None):
        """
        Initialize data extractor.
        
        Args:
            db_operations: Database operations instance (required for database sources)
        """
        self.db_operations = db_operations
    
    def extract_from_csv(self, file_path: str, csv_options: dict = None) -> pl.DataFrame:
        """
        Extract data from CSV file using Polars.
        
        Args:
            file_path: Path to CSV file
            csv_options: Additional options for CSV reading
            
        Returns:
            Polars DataFrame with CSV data
            
        Raises:
            FileNotFoundError: If CSV file doesn't exist
            Exception: If CSV reading fails
        """
        if not ValidationHelpers.validate_file_path(file_path):
            raise FileNotFoundError(f"CSV file not found or not readable: {file_path}")
        
        try:
            # Default parameters for robust CSV reading
            default_params = {
                'infer_schema_length': 10000,  # Scan more rows for better type inference
                'try_parse_dates': False,  # We'll handle datetime parsing manually
                'null_values': ['', 'NULL', 'null', 'NA', 'N/A', 'n/a'],
                'ignore_errors': True
            }
            
            # Merge with provided options
            if csv_options:
                default_params.update(csv_options)
            
            df = pl.read_csv(file_path, **default_params)
            
            # Post-process datetime columns that failed to parse
            df = self._fix_datetime_columns(df)
            
            logger.info(f"Successfully extracted CSV data: {file_path}")
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {df.columns}")
            logger.info(f"Data types: {df.dtypes}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract CSV file {file_path}: {e}")
            raise
    
    def extract_from_dbf(self, file_path: str, dbf_options: dict = None) -> pl.DataFrame:
        """
        Extract data from DBF file using the dbf package.
        
        Args:
            file_path: Path to DBF file
            dbf_options: Additional options for DBF reading
            
        Returns:
            Polars DataFrame with DBF data
            
        Raises:
            FileNotFoundError: If DBF file doesn't exist
            Exception: If DBF reading fails
        """
        if not ValidationHelpers.validate_file_path(file_path):
            raise FileNotFoundError(f"DBF file not found or not readable: {file_path}")
        
        try:
            # Read DBF file using simple approach
            with dbf.Table(file_path) as table:
                # Extract field names and data
                field_names = table.field_names
                records = []
                
                for record in table:
                    # Convert DBF record to dictionary
                    record_dict = {}
                    for field_name in field_names:
                        value = record[field_name]
                        # Handle special DBF data types
                        if isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8').strip()
                            except UnicodeDecodeError:
                                value = str(value)
                        elif value is None:
                            value = None
                        elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                            # Handle datetime objects by converting to date string
                            value = str(value.date())
                        elif hasattr(value, '__class__') and 'date' in str(type(value)).lower():
                            # Handle date objects by converting to string
                            value = str(value)
                        else:
                            value = value
                        record_dict[field_name] = value
                    records.append(record_dict)
            
            # Convert to Polars DataFrame with increased schema inference
            if records:
                # Use pandas as intermediate step for better type handling
                import pandas as pd
                pandas_df = pd.DataFrame(records)
                
                # Convert pandas DataFrame to Polars with better type inference
                df = pl.from_pandas(pandas_df)
            else:
                # Create empty DataFrame with field names
                df = pl.DataFrame({field: [] for field in field_names})
            
            # Post-process datetime columns that might need parsing
            df = self._fix_datetime_columns(df)
            
            logger.info(f"Successfully extracted DBF data: {file_path}")
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {df.columns}")
            logger.info(f"Data types: {df.dtypes}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract DBF file {file_path}: {e}")
            raise
    
    def extract_from_xlsx(self, file_path: str, xlsx_options: dict = None) -> pl.DataFrame:
        """
        Extract data from XLSX file using pandas backend.
        
        Args:
            file_path: Path to XLSX file
            xlsx_options: Additional options for XLSX reading
            
        Returns:
            Polars DataFrame with XLSX data
            
        Raises:
            FileNotFoundError: If XLSX file doesn't exist
            Exception: If XLSX reading fails
        """
        if not ValidationHelpers.validate_file_path(file_path):
            raise FileNotFoundError(f"XLSX file not found or not readable: {file_path}")
        
        try:
            # Default parameters for XLSX reading
            default_params = {
                'sheet_id': 0,  # First sheet by default (0-indexed)
                'sheet_name': None,
                'header': 0,
                'skip_rows': 0
            }
            
            # Merge with provided options
            if xlsx_options:
                default_params.update(xlsx_options)
            
            try:
                # Use pandas to read Excel file
                pandas_df = pd.read_excel(
                    file_path,
                    sheet_name=default_params.get('sheet_name') or default_params.get('sheet_id', 0),
                    header=default_params.get('header', 0),
                    skiprows=default_params.get('skip_rows', 0)
                )
                
                # Convert pandas DataFrame to Polars
                df = pl.from_pandas(pandas_df)
                logger.info(f"Successfully read XLSX using pandas and converted to Polars")
                
            except Exception as e:
                # Check if it's a missing Excel engine error  
                if "openpyxl" in str(e).lower() or "xlsxwriter" in str(e).lower() or "xlrd" in str(e).lower():
                    logger.error(f"Missing Excel engine for XLSX reading: {e}")
                    raise Exception(f"XLSX reading requires openpyxl. Install with: pip install openpyxl")
                else:
                    logger.error(f"Failed to read XLSX file: {e}")
                    raise Exception(f"Failed to read XLSX file: {e}")
            
            # Post-process datetime columns that failed to parse
            df = self._fix_datetime_columns(df)
            
            logger.info(f"Successfully extracted XLSX data: {file_path}")
            logger.info(f"DataFrame shape: {df.shape}")
            logger.info(f"Columns: {df.columns}")
            logger.info(f"Data types: {df.dtypes}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract XLSX file {file_path}: {e}")
            raise
    
    def _fix_datetime_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Fix datetime columns that failed to parse during CSV reading.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with properly parsed datetime columns
        """
        transformations = []
        datetime_patterns = [
            "%Y-%m-%d %H:%M:%S%.f",  # 2025-07-23 17:56:23.001 (correct format)
            "%Y-%m-%d %H:%M:%S",     # 2025-07-23 17:56:23
            "%Y-%m-%d",              # 2025-07-23
            "%m/%d/%Y %H:%M:%S%.f",  # 07/23/2025 17:56:23.001
            "%m/%d/%Y %H:%M:%S",     # 07/23/2025 17:56:23
            "%m/%d/%Y",              # 07/23/2025
        ]
        
        for col in df.columns:
            col_data = df[col]
            
            # Check if column contains datetime-like strings
            if (col_data.dtype == pl.Utf8 and 
                col.strip().lower().endswith(('_at', '_time', 'date', 'time')) and
                not col_data.is_null().all()):
                
                # Try parsing with different patterns
                parsed_col = None
                for pattern in datetime_patterns:
                    try:
                        # Strip whitespace from values before parsing
                        parsed_col = pl.col(col).str.strip_chars().str.strptime(pl.Datetime, pattern, strict=False)
                        # Test if parsing works by applying to first non-null value
                        test_df = df.select(parsed_col.alias("test"))
                        if not test_df["test"].is_null().all():
                            logger.info(f"Successfully parsed datetime column '{col}' with pattern '{pattern}'")
                            transformations.append(parsed_col.alias(col))
                            break
                    except Exception as e:
                        logger.debug(f"Pattern '{pattern}' failed for column '{col}': {e}")
                        continue
                
                if parsed_col is None:
                    logger.warning(f"Could not parse datetime column '{col}', keeping as string")
                    transformations.append(pl.col(col))
            else:
                transformations.append(pl.col(col))
        
        if len(transformations) > 0:
            return df.with_columns(transformations)
        return df
    
    def extract_from_sql_file(self, sql_file_path: str) -> str:
        """
        Read SQL query from file.
        
        Args:
            sql_file_path: Path to SQL file
            
        Returns:
            SQL query string
            
        Raises:
            FileNotFoundError: If SQL file doesn't exist
            ValueError: If SQL file is empty
        """
        if not ValidationHelpers.validate_file_path(sql_file_path):
            raise FileNotFoundError(f"SQL file not found or not readable: {sql_file_path}")
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_query = f.read().strip()
            
            if not sql_query:
                raise ValueError(f"SQL file is empty: {sql_file_path}")
            
            logger.info(f"Successfully read SQL file: {sql_file_path}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Failed to read SQL file {sql_file_path}: {e}")
            raise
    
    def extract_from_custom_query(self, sql_query: str) -> pl.DataFrame:
        """
        Execute custom SQL query and return results.
        
        Args:
            sql_query: SQL query string
            
        Returns:
            Polars DataFrame with query results
            
        Raises:
            RuntimeError: If database operations not available
            Exception: If query execution fails
        """
        if not self.db_operations:
            raise RuntimeError("Database operations not available for custom query extraction")
        
        try:
            df = self.db_operations.execute_query(sql_query)
            logger.info(f"Successfully extracted data from custom query, {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Failed to execute custom query: {e}")
            raise
    
    def extract_from_table(self, source_table: str, source_schema: Optional[str] = None) -> pl.DataFrame:
        """
        Extract data from existing Oracle table.
        
        Args:
            source_table: Source table name
            source_schema: Optional schema name
            
        Returns:
            Polars DataFrame with table data
            
        Raises:
            RuntimeError: If database operations not available
            Exception: If table extraction fails
        """
        if not self.db_operations:
            raise RuntimeError("Database operations not available for table extraction")
        
        try:
            # Build query for source table
            schema_prefix = f"{source_schema}." if source_schema else ""
            query = f"SELECT * FROM {schema_prefix}{source_table}"
            
            df = self.db_operations.execute_query(query)
            logger.info(f"Successfully extracted data from table {schema_prefix}{source_table}, {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract from table {source_table}: {e}")
            raise
    
    def extract_data(self, config: TableConfig, file_path: Optional[str] = None) -> pl.DataFrame:
        """
        Extract data from various sources based on configuration.
        
        Args:
            config: TableConfig object with source configuration
            file_path: Path to source file (for CSV sources)
            
        Returns:
            Polars DataFrame with source data
            
        Raises:
            ValueError: If configuration is invalid or required parameters missing
            Exception: If data extraction fails
        """
        source_type = config.source_type.lower()
        
        if source_type == 'csv':
            if not file_path:
                raise ValueError("file_path is required for CSV source type")
            return self.extract_from_csv(file_path, config.csv_options)
        
        elif source_type == 'dbf':
            if not file_path:
                raise ValueError("file_path is required for DBF source type")
            return self.extract_from_dbf(file_path, config.dbf_options)
        
        elif source_type == 'xlsx':
            if not file_path:
                raise ValueError("file_path is required for XLSX source type")
            return self.extract_from_xlsx(file_path, config.xlsx_options)
        
        elif source_type == 'custom_query':
            if not config.custom_query_file:
                raise ValueError("custom_query file path is required for custom_query source type")
            
            return self.extract_from_custom_query_with_source(config, file_path)
        
        elif source_type == 'table':
            if not config.source_table:
                raise ValueError("source_table is required for table source type")
            
            return self.extract_from_table(config.source_table, config.source_schema)
        
        else:
            raise ValueError(f"Unsupported source type: {config.source_type}")
    
    def validate_extracted_data(self, df: pl.DataFrame, config: TableConfig) -> bool:
        """
        Validate extracted data before processing.
        
        Args:
            df: Extracted DataFrame
            config: Table configuration
            
        Returns:
            True if data is valid
            
        Raises:
            ValueError: If data validation fails
        """
        if df.is_empty():
            raise ValueError("Extracted DataFrame is empty")
        
        if len(df.columns) == 0:
            raise ValueError("Extracted DataFrame has no columns")
        
        # Create case-insensitive column lookup for flexible matching
        df_columns_lower = [col.strip().lower() for col in df.columns]
        
        def find_column_case_insensitive(target_col):
            """Find column in DataFrame with case-insensitive matching."""
            target_lower = target_col.lower()
            for i, col_lower in enumerate(df_columns_lower):
                if col_lower == target_lower:
                    return df.columns[i]
            return None
        
        # Validate primary key columns exist (case-insensitive)
        if config.primary_key:
            missing_pk_cols = []
            for col in config.primary_key:
                if find_column_case_insensitive(col) is None:
                    missing_pk_cols.append(col)
            if missing_pk_cols:
                logger.error(f"Available columns: {df.columns}")
                logger.error(f"Looking for primary key columns: {config.primary_key}")
                raise ValueError(f"Primary key columns not found in data: {missing_pk_cols}")
        
        # Validate foreign key columns exist (case-insensitive)
        if config.foreign_keys:
            for fk in config.foreign_keys:
                if fk.get('columns'):  # Skip empty foreign keys
                    missing_fk_cols = []
                    for col in fk['columns']:
                        if find_column_case_insensitive(col) is None:
                            missing_fk_cols.append(col)
                    if missing_fk_cols:
                        raise ValueError(f"Foreign key columns not found in data: {missing_fk_cols}")
        
        # Validate index columns exist (case-insensitive)
        if config.indexes:
            for idx in config.indexes:
                if idx.get('columns'):  # Skip empty indexes
                    missing_idx_cols = []
                    for col in idx['columns']:
                        if find_column_case_insensitive(col) is None:
                            missing_idx_cols.append(col)
                    if missing_idx_cols:
                        raise ValueError(f"Index columns not found in data: {missing_idx_cols}")
        
        logger.info(f"Data validation passed for {len(df)} rows, {len(df.columns)} columns")
        return True
    
    def extract_from_custom_query_with_source(self, config: TableConfig, file_path: Optional[str] = None) -> pl.DataFrame:
        """
        Execute custom SQL query with data source (CSV or table) using DuckDB.
        
        Args:
            config: Table configuration with data_source information
            file_path: Optional file path (used for CSV data sources if data_source.file_path not specified)
            
        Returns:
            Polars DataFrame with query results
            
        Raises:
            ValueError: If data source configuration is invalid
            Exception: If query execution fails
        """
        try:
            # Read the SQL query
            sql_query = self.extract_from_sql_file(config.custom_query_file)
            
            # Get data source configuration
            data_source = config.data_source
            ds_type = data_source.get('type', '').lower()
            
            # Load source data based on type
            if ds_type == 'csv':
                # Use file_path from data_source or fallback to parameter
                csv_file_path = data_source.get('file_path', file_path)
                if not csv_file_path:
                    raise ValueError("No CSV file path specified in data_source or parameter")
                
                # Get CSV options from data_source or config
                csv_options = data_source.get('options', getattr(config, 'csv_options', {}))
                source_df = self.extract_from_csv(csv_file_path, csv_options)
            
            elif ds_type == 'dbf':
                # Use file_path from data_source or fallback to parameter
                dbf_file_path = data_source.get('file_path', file_path)
                if not dbf_file_path:
                    raise ValueError("No DBF file path specified in data_source or parameter")
                
                # Get DBF options from data_source or config
                dbf_options = data_source.get('options', getattr(config, 'dbf_options', {}))
                source_df = self.extract_from_dbf(dbf_file_path, dbf_options)
                
            elif ds_type == 'xlsx':
                # Use file_path from data_source or fallback to parameter
                xlsx_file_path = data_source.get('file_path', file_path)
                if not xlsx_file_path:
                    raise ValueError("No XLSX file path specified in data_source or parameter")
                
                # Get XLSX options from data_source or config
                xlsx_options = data_source.get('options', getattr(config, 'xlsx_options', {}))
                source_df = self.extract_from_xlsx(xlsx_file_path, xlsx_options)
                
            elif ds_type == 'table':
                # Extract from database table
                table_name = data_source.get('table_name')
                schema_name = data_source.get('schema_name')
                
                if not self.db_operations:
                    raise RuntimeError("Database operations not available for table data source")
                
                source_df = self.extract_from_table(table_name, schema_name)
                
            else:
                raise ValueError(f"Unsupported data source type: {ds_type}")
            
            # Clean up column names before executing query (trim whitespace)
            trimmed_columns = [col.strip() for col in source_df.columns]
            if trimmed_columns != source_df.columns:
                column_rename_map = {old: new for old, new in zip(source_df.columns, trimmed_columns)}
                source_df = source_df.rename(column_rename_map)
                logger.info(f"Trimmed whitespace from source data column names: {column_rename_map}")
            
            # Execute custom query using DuckDB on the source data
            result_df = self._execute_query_on_dataframe(source_df, sql_query, data_source)
            
            logger.info(f"Custom query with data source executed successfully, returned {len(result_df)} rows")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute custom query with data source: {e}")
            raise
    
    def _execute_query_on_dataframe(self, df: pl.DataFrame, sql_query: str, data_source: dict) -> pl.DataFrame:
        """
        Execute SQL query on DataFrame using DuckDB.
        
        Args:
            df: Source DataFrame
            sql_query: SQL query to execute
            data_source: Data source configuration (contains table alias info)
            
        Returns:
            Polars DataFrame with query results
        """
        try:
            # Create DuckDB connection
            conn = duckdb.connect()
            
            # Get table alias from data_source or use default
            table_alias = data_source.get('table_alias', 'source_table')
            
            # Convert Polars DataFrame to DuckDB relation and register it
            conn.register(table_alias, df.to_arrow())
            
            # Replace table references in SQL query
            # This is a simple replacement - in a production system you might want more sophisticated parsing
            modified_query = sql_query
            
            # If the query references a specific table name, we need to handle the mapping
            # For now, we'll assume the query is written to use the table_alias
            
            # Execute the query
            result = conn.execute(modified_query).fetchall()
            columns = [desc[0] for desc in conn.description]
            
            # Convert back to Polars DataFrame
            if result:
                records = [dict(zip(columns, row)) for row in result]
                result_df = pl.DataFrame(records)
            else:
                result_df = pl.DataFrame({col: [] for col in columns})
            
            conn.close()
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute query on DataFrame: {e}")
            raise