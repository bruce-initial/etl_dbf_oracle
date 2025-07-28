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
            
            logger.info(f"Successfully extracted CSV: {file_path} ({len(df)} rows)")
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
                            # Try multiple encodings for DBF files
                            for encoding in ['utf-8', 'big5', 'gb2312', 'gbk', 'latin1', 'cp1252', 'iso-8859-1']:
                                try:
                                    value = value.decode(encoding).strip()
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                # If all encodings fail, use error handling
                                value = value.decode('utf-8', errors='replace').strip()
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
            
            # Convert to Polars DataFrame with better type handling
            if records:
                # Use pandas as intermediate step for better type handling
                import pandas as pd
                pandas_df = pd.DataFrame(records)
                
                # Ensure proper type inference for mixed string/numeric data
                for col in pandas_df.columns:
                    # Try to convert numeric strings to actual numbers
                    if pandas_df[col].dtype == 'object':
                        # Check if all non-null values are numeric strings
                        non_null_values = pandas_df[col].dropna()
                        if len(non_null_values) > 0:
                            try:
                                # Try converting to numeric
                                numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                                # If all values converted successfully (no NaN from conversion)
                                if not numeric_values.isna().any():
                                    pandas_df[col] = pd.to_numeric(pandas_df[col], errors='coerce')
                            except:
                                pass  # Keep as string
                
                # Convert pandas DataFrame to Polars
                df = pl.from_pandas(pandas_df)
                
            else:
                # Create empty DataFrame with field names
                df = pl.DataFrame({field: [] for field in field_names})
            
            # Post-process datetime columns that might need parsing
            df = self._fix_datetime_columns(df)
            
            logger.info(f"Successfully extracted DBF: {file_path} ({len(df)} rows)")
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
            
            logger.info(f"Successfully extracted XLSX: {file_path} ({len(df)} rows)")
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
            if isinstance(file_path, list):
                return self.extract_from_multiple_csv(file_path, config.csv_options)
            else:
                return self.extract_from_csv(file_path, config.csv_options)
        
        elif source_type == 'dbf':
            if not file_path:
                raise ValueError("file_path is required for DBF source type")
            if isinstance(file_path, list):
                return self.extract_from_multiple_dbf(file_path, config.dbf_options)
            else:
                return self.extract_from_dbf(file_path, config.dbf_options)
        
        elif source_type == 'xlsx':
            if not file_path:
                raise ValueError("file_path is required for XLSX source type")
            if isinstance(file_path, list):
                return self.extract_from_multiple_xlsx(file_path, config.xlsx_options)
            else:
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
                
                # Handle multiple XLSX files for custom queries
                if isinstance(xlsx_file_path, list):
                    source_df = self._prepare_multiple_xlsx_files(xlsx_file_path, xlsx_options, data_source)
                else:
                    source_df = self.extract_from_xlsx(xlsx_file_path, xlsx_options)
                
            elif ds_type == 'table':
                # Extract from database table(s)
                table_name = data_source.get('table_name')
                schema_name = data_source.get('schema_name')
                
                if not self.db_operations:
                    raise RuntimeError("Database operations not available for table data source")
                
                # Handle multiple tables for custom queries
                if isinstance(table_name, list):
                    # For multiple tables, we'll register each table individually with DuckDB
                    # The actual query execution will be handled by the custom query
                    source_df = self._prepare_multiple_oracle_tables(table_name, schema_name)
                else:
                    # Single table extraction
                    if not table_name:
                        raise ValueError("table_name is required for table data source")
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
            df: Source DataFrame or None (for Oracle table data sources)
            sql_query: SQL query to execute  
            data_source: Data source configuration (contains table alias info)
            
        Returns:
            Polars DataFrame with query results
        """
        try:
            logger.debug(f"Executing query on DataFrame with data_source type: {data_source.get('type')}")
            logger.debug(f"DataFrame attributes: {[attr for attr in dir(df) if attr.startswith('_')]}")
            
            # Handle Oracle table data sources differently
            if data_source.get('type') == 'table' and hasattr(df, '_oracle_tables'):
                logger.debug("Using Oracle table data source")
                return self._execute_oracle_custom_query(sql_query)
            
            # Handle multi-file data sources
            if '_multi_file_marker' in df.columns:
                logger.debug("Using multi-file data source")
                # Use the pre-configured DuckDB connection with multiple tables
                multi_conn = df._duckdb_connection
                
                try:
                    # Execute the query on the multi-file setup
                    result = multi_conn.execute(sql_query).fetchall()
                    columns = [desc[0] for desc in multi_conn.description]
                    
                    # Convert back to Polars DataFrame
                    if result:
                        records = [dict(zip(columns, row)) for row in result]
                        result_df = pl.DataFrame(records)
                    else:
                        result_df = pl.DataFrame({col: [] for col in columns})
                    
                    return result_df
                    
                except Exception as e:
                    logger.error(f"Failed to execute multi-file query: {e}")
                    raise
                finally:
                    # Clean up the connection
                    try:
                        multi_conn.close()
                    except:
                        pass
            
            # Create DuckDB connection for single file sources
            conn = duckdb.connect()
            
            # For file-based data sources, use DuckDB
            # Get table alias from data_source or use default
            table_alias = data_source.get('table_alias', 'source_table')
            logger.debug(f"Using single file data source with alias: {table_alias}")
            
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
    
    def _prepare_multiple_oracle_tables(self, table_names: list, schema_name: Optional[str] = None) -> pl.DataFrame:
        """
        Prepare marker DataFrame for multiple Oracle tables (used for custom queries).
        
        Args:
            table_names: List of Oracle table names
            schema_name: Optional schema name
            
        Returns:
            Marker DataFrame with table information
        """
        # Create a marker DataFrame that indicates this is an Oracle table data source
        marker_df = pl.DataFrame({'_oracle_table_marker': [True]})
        # Store the table information as attributes
        marker_df._oracle_tables = table_names
        marker_df._oracle_schema = schema_name
        
        logger.info(f"Prepared Oracle table data source with tables: {table_names}")
        return marker_df
    
    def _execute_oracle_custom_query(self, sql_query: str) -> pl.DataFrame:
        """
        Execute custom SQL query directly on Oracle database.
        
        Args:
            sql_query: SQL query to execute on Oracle
            
        Returns:
            Polars DataFrame with query results
        """
        if not self.db_operations:
            raise RuntimeError("Database operations not available for Oracle custom query")
        
        try:
            # Execute the query directly on Oracle
            result_df = self.db_operations.execute_query(sql_query)
            logger.info(f"Oracle custom query executed successfully, returned {len(result_df)} rows")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute Oracle custom query: {e}")
            raise
    
    def _prepare_multiple_xlsx_files(self, file_paths: list, xlsx_options: dict, data_source: dict) -> pl.DataFrame:
        """
        Prepare multiple XLSX files for custom query execution.
        
        Args:
            file_paths: List of XLSX file paths
            xlsx_options: XLSX reading options
            data_source: Data source configuration
            
        Returns:
            Combined DataFrame or marker DataFrame
        """
        try:
            # Create DuckDB connection for combining files
            conn = duckdb.connect()
            
            # Get table aliases - should match the number of files
            table_aliases = data_source.get('table_alias', [])
            if isinstance(table_aliases, str):
                table_aliases = [table_aliases]
            
            # Ensure we have aliases for all files
            if len(table_aliases) != len(file_paths):
                # Generate default aliases if not provided or mismatched
                table_aliases = [f"source_table_{i+1}" for i in range(len(file_paths))]
                logger.warning(f"Generated default table aliases: {table_aliases}")
            
            # Load each XLSX file and register with DuckDB
            for i, (file_path, alias) in enumerate(zip(file_paths, table_aliases)):
                try:
                    # Extract the XLSX file
                    df = self.extract_from_xlsx(file_path, xlsx_options)
                    
                    # Register with DuckDB - register each table individually
                    conn.register(str(alias), df.to_arrow())
                    logger.info(f"Registered XLSX file {file_path} as table '{alias}' with {len(df)} rows")
                    
                except Exception as e:
                    logger.error(f"Failed to load XLSX file {file_path}: {e}")
                    raise
            
            # Store connection and aliases for later use in query execution
            # Create a marker DataFrame that indicates this is a multi-file source
            marker_df = pl.DataFrame({'_multi_file_marker': [True]})
            marker_df._duckdb_connection = conn
            marker_df._table_aliases = table_aliases
            marker_df._file_paths = file_paths
            
            logger.info(f"Prepared multiple XLSX files: {file_paths} with aliases: {table_aliases}")
            return marker_df
            
        except Exception as e:
            logger.error(f"Failed to prepare multiple XLSX files: {e}")
            raise
    
    def extract_from_multiple_csv(self, file_paths: list, csv_options: dict = None) -> pl.DataFrame:
        """
        Extract and combine data from multiple CSV files.
        
        Args:
            file_paths: List of CSV file paths
            csv_options: Additional options for CSV reading
            
        Returns:
            Combined Polars DataFrame with data from all CSV files
            
        Raises:
            FileNotFoundError: If any CSV file doesn't exist
            Exception: If CSV reading fails
        """
        try:
            logger.info(f"Starting batch extraction: {len(file_paths)} CSV files")
            
            dataframes = []
            for i, file_path in enumerate(file_paths):
                batch_num = i+1
                logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                logger.info(f"Status: Extracting batch {batch_num}")
                df = self.extract_from_csv(file_path, csv_options)
                logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")
                
                # Add source file column to track origin
                df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                dataframes.append(df)
            
            # Combine all dataframes
            if len(dataframes) == 1:
                combined_df = dataframes[0]
            else:
                # Use concat to combine dataframes with schema alignment
                combined_df = pl.concat(dataframes, how="diagonal_relaxed")
            
            logger.info(f"Batch processing complete: {len(file_paths)} CSV files, {len(combined_df)} total rows")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to extract multiple CSV files: {e}")
            raise
    
    def extract_from_multiple_dbf(self, file_paths: list, dbf_options: dict = None) -> pl.DataFrame:
        """
        Extract and combine data from multiple DBF files.
        
        Args:
            file_paths: List of DBF file paths
            dbf_options: Additional options for DBF reading
            
        Returns:
            Combined Polars DataFrame with data from all DBF files
            
        Raises:
            FileNotFoundError: If any DBF file doesn't exist
            Exception: If DBF reading fails
        """
        try:
            logger.info(f"Starting batch extraction: {len(file_paths)} DBF files")
            
            dataframes = []
            for i, file_path in enumerate(file_paths):
                batch_num = i+1
                logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                logger.info(f"Status: Extracting batch {batch_num}")
                df = self.extract_from_dbf(file_path, dbf_options)
                logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")
                
                # Add source file column to track origin
                df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                dataframes.append(df)
            
            # Combine all dataframes
            if len(dataframes) == 1:
                combined_df = dataframes[0]
            else:
                # Use concat to combine dataframes with schema alignment
                combined_df = pl.concat(dataframes, how="diagonal_relaxed")
            
            logger.info(f"Batch processing complete: {len(file_paths)} DBF files, {len(combined_df)} total rows")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to extract multiple DBF files: {e}")
            raise
    
    def extract_from_multiple_xlsx(self, file_paths: list, xlsx_options: dict = None) -> pl.DataFrame:
        """
        Extract and combine data from multiple XLSX files.
        
        Args:
            file_paths: List of XLSX file paths
            xlsx_options: Additional options for XLSX reading
            
        Returns:
            Combined Polars DataFrame with data from all XLSX files
            
        Raises:
            FileNotFoundError: If any XLSX file doesn't exist
            Exception: If XLSX reading fails
        """
        try:
            logger.info(f"Starting batch extraction: {len(file_paths)} XLSX files")
            
            dataframes = []
            for i, file_path in enumerate(file_paths):
                batch_num = i+1
                logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                logger.info(f"Status: Extracting batch {batch_num}")
                df = self.extract_from_xlsx(file_path, xlsx_options)
                logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")
                
                # Add source file column to track origin
                df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                dataframes.append(df)
            
            # Combine all dataframes
            if len(dataframes) == 1:
                combined_df = dataframes[0]
            else:
                # Use concat to combine dataframes with schema alignment
                combined_df = pl.concat(dataframes, how="diagonal_relaxed")
            
            logger.info(f"Batch processing complete: {len(file_paths)} XLSX files, {len(combined_df)} total rows")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to extract multiple XLSX files: {e}")
            raise