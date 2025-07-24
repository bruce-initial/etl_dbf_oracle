"""
Database operations for Oracle ETL pipeline.
"""

import oracledb
import polars as pl
import logging
from typing import Dict, List, Tuple, Any
from .connection import OracleConnection
from .schema import SchemaManager
from ..utils.helpers import ColumnSanitizer

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Handles Oracle database operations for ETL pipeline."""
    
    def __init__(self, connection: OracleConnection):
        """
        Initialize database operations.
        
        Args:
            connection: Oracle connection instance
        """
        self.connection = connection
        self.schema_manager = SchemaManager()
    
    def create_table(self, table_name: str, df: pl.DataFrame, 
                    drop_if_exists: bool = True) -> Dict[str, str]:
        """
        Create Oracle table based on DataFrame structure.
        
        Args:
            table_name: Name of the table to create
            df: Polars DataFrame with the data
            drop_if_exists: Whether to drop table if it already exists
            
        Returns:
            Column mapping dictionary
            
        Raises:
            oracledb.Error: If table creation fails
        """
        try:
            oracle_conn = self.connection.get_connection()
            cursor = oracle_conn.cursor()
            
            # Drop table if it exists and requested
            if drop_if_exists:
                try:
                    cursor.execute(f"DROP TABLE {table_name}")
                    logger.info(f"Dropped existing table: {table_name}")
                except oracledb.DatabaseError:
                    pass  # Table doesn't exist, which is fine
            
            # Sanitize column names
            column_mapping = ColumnSanitizer.sanitize_column_names(df.columns)
            
            # Generate and execute DDL
            ddl = self.schema_manager.create_table_ddl(table_name, df, column_mapping)
            logger.info(f"Creating table with DDL:\n{ddl}")
            
            cursor.execute(ddl)
            oracle_conn.commit()
            
            logger.info(f"Successfully created table: {table_name}")
            cursor.close()
            
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def create_primary_key(self, table_name: str, primary_key_columns: List[str]) -> None:
        """
        Create primary key constraint on table.
        
        Args:
            table_name: Target table name
            primary_key_columns: List of primary key column names
        """
        if not primary_key_columns:
            return
        
        try:
            oracle_conn = self.connection.get_connection()
            cursor = oracle_conn.cursor()
            
            ddl = self.schema_manager.create_primary_key_ddl(table_name, primary_key_columns)
            cursor.execute(ddl)
            oracle_conn.commit()
            cursor.close()
            
            pk_columns = ", ".join(primary_key_columns)
            logger.info(f"Created primary key on {table_name}: {pk_columns}")
            
        except Exception as e:
            logger.warning(f"Failed to create primary key on {table_name}: {e}")
    
    def create_foreign_keys(self, table_name: str, foreign_keys: List[Dict[str, Any]]) -> None:
        """
        Create foreign key constraints on table.
        
        Args:
            table_name: Target table name
            foreign_keys: List of foreign key definitions
                         Format: [{'columns': ['col1'], 'reference_table': 'ref_table', 'reference_columns': ['ref_col1']}]
        """
        if not foreign_keys:
            return
        
        oracle_conn = self.connection.get_connection()
        cursor = oracle_conn.cursor()
        
        for i, fk in enumerate(foreign_keys):
            try:
                ddl = self.schema_manager.create_foreign_key_ddl(table_name, fk, i)
                cursor.execute(ddl)
                
                fk_columns = ", ".join(fk['columns'])
                ref_table = fk['reference_table']
                ref_columns = ", ".join(fk['reference_columns'])
                logger.info(f"Created foreign key on {table_name}: {fk_columns} -> {ref_table}({ref_columns})")
                
            except Exception as e:
                logger.warning(f"Failed to create foreign key {i+1} on {table_name}: {e}")
        
        oracle_conn.commit()
        cursor.close()
    
    def create_indexes(self, table_name: str, indexes: List[Dict[str, Any]]) -> None:
        """
        Create indexes on table.
        
        Args:
            table_name: Target table name
            indexes: List of index definitions
                    Format: [{'name': 'idx_name', 'columns': ['col1', 'col2'], 'unique': False}]
        """
        if not indexes:
            return
        
        oracle_conn = self.connection.get_connection()
        cursor = oracle_conn.cursor()
        
        for idx in indexes:
            try:
                ddl = self.schema_manager.create_index_ddl(table_name, idx)
                cursor.execute(ddl)
                
                idx_name = idx.get('name', f"IDX_{table_name}_{len(idx['columns'])}")
                logger.info(f"Created {'unique ' if idx.get('unique') else ''}index on {table_name}: {idx_name}")
                
            except Exception as e:
                logger.warning(f"Failed to create index {idx.get('name', 'unnamed')} on {table_name}: {e}")
        
        oracle_conn.commit()
        cursor.close()
    
    def prepare_data_for_insert(self, df: pl.DataFrame, 
                               column_mapping: Dict[str, str]) -> Tuple[List[str], List[List]]:
        """
        Prepare data for Oracle insertion with proper type conversion and validation.
        
        Args:
            df: Polars DataFrame
            column_mapping: Column name mapping
            
        Returns:
            Tuple of (column_names, data_rows)
        """
        # Rename columns according to mapping
        df_renamed = df.rename(column_mapping)
        
        # Get column types for proper conversion
        column_types = {col: dtype for col, dtype in zip(df_renamed.columns, df_renamed.dtypes)}
        
        # Log column types for debugging
        logger.debug(f"DataFrame column types for Oracle insertion: {column_types}")
        
        # Validate data types and log potential issues
        self._validate_data_types_for_oracle(df_renamed, column_types)
        
        # Convert to list of lists for Oracle insertion
        # Handle None/null values and type conversion properly
        data_rows = []
        type_conversion_stats = {}
        
        for row_idx, row in enumerate(df_renamed.iter_rows()):
            processed_row = []
            for i, (value, col_name) in enumerate(zip(row, df_renamed.columns)):
                if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
                    processed_row.append(None)
                else:
                    # Convert value based on expected Oracle type
                    col_type = column_types[col_name]
                    original_type = type(value).__name__
                    processed_value = self._convert_value_for_oracle(value, col_type)
                    processed_type = type(processed_value).__name__
                    
                    # Track type conversions for statistics
                    conversion_key = f"{col_name}:{original_type}->{processed_type}"
                    type_conversion_stats[conversion_key] = type_conversion_stats.get(conversion_key, 0) + 1
                    
                    processed_row.append(processed_value)
            data_rows.append(processed_row)
        
        column_names = list(column_mapping.values())
        
        # Log type conversion statistics
        if type_conversion_stats:
            logger.debug("Type conversion statistics:")
            for conversion, count in sorted(type_conversion_stats.items()):
                if count > 1:  # Only log conversions that happened multiple times
                    logger.debug(f"  {conversion}: {count} times")
        
        logger.debug(f"Prepared {len(data_rows)} rows for insertion")
        return column_names, data_rows
    
    def _validate_data_types_for_oracle(self, df: pl.DataFrame, column_types: Dict[str, pl.DataType]) -> None:
        """
        Validate data types before Oracle insertion and log potential issues.
        
        Args:
            df: DataFrame to validate
            column_types: Column type mapping
        """
        issues = []
        
        for col_name, col_type in column_types.items():
            # Sample some values to check for type consistency
            sample_values = df[col_name].head(100).to_list()
            non_null_values = [v for v in sample_values if v is not None]
            
            if not non_null_values:
                continue
                
            # Check for mixed types that might cause issues
            python_types = set(type(v).__name__ for v in non_null_values)
            
            if len(python_types) > 2:  # More than just None + one type
                issues.append(f"Column '{col_name}' has mixed types: {python_types}")
            
            # Check for potential Oracle type mismatches
            if col_type == pl.Utf8:
                # Check if any values are actually numbers
                numeric_values = [v for v in non_null_values if isinstance(v, (int, float))]
                if numeric_values:
                    issues.append(f"Column '{col_name}' (VARCHAR) contains {len(numeric_values)} numeric values")
            
            elif col_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
                # Check if any values are floats or strings
                problematic_values = [v for v in non_null_values if not isinstance(v, int)]
                if problematic_values[:3]:  # Show first 3 examples
                    issues.append(f"Column '{col_name}' (INTEGER) contains non-integer values: {problematic_values[:3]}")
            
            elif isinstance(col_type, (pl.Date, pl.Datetime)):
                # Check for date format issues
                string_dates = [v for v in non_null_values if isinstance(v, str)]
                if string_dates:
                    # Sample a few date strings to check format
                    sample_dates = string_dates[:5]
                    unparseable_dates = []
                    for date_str in sample_dates:
                        try:
                            import datetime
                            # Try common date patterns
                            date_patterns = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y']
                            parsed = False
                            for pattern in date_patterns:
                                try:
                                    datetime.datetime.strptime(date_str.strip(), pattern)
                                    parsed = True
                                    break
                                except ValueError:
                                    continue
                            if not parsed:
                                unparseable_dates.append(date_str)
                        except:
                            unparseable_dates.append(date_str)
                    
                    if unparseable_dates:
                        issues.append(f"Column '{col_name}' (DATE) contains unparseable date strings: {unparseable_dates}")
        
        if issues:
            logger.warning("Potential data type issues detected:")
            for issue in issues:
                logger.warning(f"  {issue}")
            logger.warning("These issues will be handled during type conversion, but may affect data quality.")
    
    def _convert_value_for_oracle(self, value, polars_type):
        """
        Convert value to appropriate type for Oracle insertion.
        
        Args:
            value: The value to convert
            polars_type: Polars data type
            
        Returns:
            Converted value suitable for Oracle
        """
        if value is None:
            return None
            
        try:
            # Handle string types - keep as string
            if polars_type == pl.Utf8:
                return str(value)
            
            # Handle numeric types
            elif polars_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                               pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                # Convert to int, handle string representations
                if isinstance(value, str):
                    try:
                        return int(float(value))  # Handle "123.0" strings
                    except (ValueError, TypeError):
                        return None
                return int(value)
            
            # Handle float types
            elif polars_type in [pl.Float32, pl.Float64]:
                if isinstance(value, str):
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None
                return float(value)
            
            # Handle boolean types
            elif polars_type == pl.Boolean:
                if isinstance(value, str):
                    return 1 if value.lower() in ['true', '1', 'yes', 'y'] else 0
                return 1 if value else 0
            
            # Handle date/datetime types
            elif isinstance(polars_type, (pl.Date, pl.Datetime)):
                return self._convert_date_for_oracle(value)
            
            # Default: convert to string
            else:
                return str(value)
                
        except Exception as e:
            logger.warning(f"Failed to convert value {value} of type {type(value)} for Polars type {polars_type}: {e}")
            return str(value)  # Fallback to string
    
    def _convert_date_for_oracle(self, value):
        """
        Convert date/datetime values to Oracle-compatible format.
        
        Args:
            value: Date/datetime value to convert
            
        Returns:
            Oracle-compatible date value or None
        """
        if value is None:
            return None
            
        try:
            import datetime
            
            # Handle string dates
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                
                # Common date patterns to try
                date_patterns = [
                    '%Y-%m-%d',           # 2006-11-06
                    '%Y-%m-%d %H:%M:%S',  # 2006-11-06 12:30:45
                    '%Y-%m-%d %H:%M:%S.%f', # 2006-11-06 12:30:45.123456
                    '%m/%d/%Y',           # 11/06/2006
                    '%m/%d/%Y %H:%M:%S',  # 11/06/2006 12:30:45
                    '%d/%m/%Y',           # 06/11/2006
                    '%d/%m/%Y %H:%M:%S',  # 06/11/2006 12:30:45
                    '%d-%m-%Y',           # 06-11-2006
                    '%d-%m-%Y %H:%M:%S',  # 06-11-2006 12:30:45
                ]
                
                parsed_date = None
                for pattern in date_patterns:
                    try:
                        parsed_date = datetime.datetime.strptime(value, pattern)
                        break
                    except ValueError:
                        continue
                
                if parsed_date:
                    # Return in Oracle's preferred format
                    return parsed_date
                else:
                    logger.warning(f"Could not parse date string '{value}', returning as-is")
                    return value
            
            # Handle datetime objects
            elif hasattr(value, 'date') and callable(getattr(value, 'date')):
                # Convert datetime to date if needed
                if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                    return value
                else:
                    return datetime.datetime.combine(value.date(), datetime.time())
            
            # Handle date objects
            elif hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                # Convert date to datetime for Oracle
                return datetime.datetime.combine(value, datetime.time())
            
            # Handle numeric timestamps
            elif isinstance(value, (int, float)):
                try:
                    return datetime.datetime.fromtimestamp(value)
                except (ValueError, OSError):
                    logger.warning(f"Could not convert timestamp {value} to date")
                    return None
            
            else:
                # Last resort: convert to string
                return str(value)
                
        except Exception as e:
            logger.warning(f"Error converting date value {value}: {e}")
            return None
    
    def _insert_batch_with_recovery(self, cursor, insert_sql: str, batch: List[List], 
                                   column_names: List[str], df: pl.DataFrame, 
                                   column_mapping: Dict[str, str], batch_start_idx: int) -> int:
        """
        Insert batch rows one by one with aggressive type conversion for error recovery.
        
        Args:
            cursor: Oracle cursor
            insert_sql: INSERT SQL statement
            batch: Failed batch data
            column_names: Column names for the table
            df: Original DataFrame for type reference
            column_mapping: Column mapping
            batch_start_idx: Starting index of the batch in original data
            
        Returns:
            Number of successfully inserted rows
        """
        recovered_count = 0
        
        for row_idx, row in enumerate(batch):
            try:
                # Try inserting the row as-is first
                cursor.execute(insert_sql, row)
                recovered_count += 1
                
            except Exception as row_error:
                logger.debug(f"Row {batch_start_idx + row_idx} failed: {row_error}")
                
                # Check if it's a type mismatch or date format error
                if ("DPY-3013" in str(row_error) or "unsupported Python type" in str(row_error) or 
                    "ORA-01843" in str(row_error) or "not a valid month" in str(row_error)):
                    # Apply aggressive type conversion
                    converted_row = self._convert_row_for_oracle_recovery(row, column_names, df, column_mapping)
                    
                    try:
                        cursor.execute(insert_sql, converted_row)
                        recovered_count += 1
                        logger.debug(f"Successfully recovered row {batch_start_idx + row_idx} with type conversion")
                        
                    except Exception as recovery_error:
                        logger.warning(f"Failed to recover row {batch_start_idx + row_idx} even with type conversion: {recovery_error}")
                        # Skip this row - could not be recovered
                        continue
                else:
                    logger.warning(f"Non-recoverable error for row {batch_start_idx + row_idx}: {row_error}")
                    continue
        
        return recovered_count
    
    def _convert_row_for_oracle_recovery(self, row: List, column_names: List[str], 
                                        df: pl.DataFrame, column_mapping: Dict[str, str]) -> List:
        """
        Aggressively convert row values to strings for Oracle insertion recovery.
        
        Args:
            row: Row data that failed insertion
            column_names: Column names
            df: Original DataFrame
            column_mapping: Column mapping
            
        Returns:
            Row with all values converted to appropriate types for Oracle
        """
        converted_row = []
        
        # Create reverse mapping to get original column names
        reverse_mapping = {v: k for k, v in column_mapping.items()}
        
        for i, value in enumerate(row):
            try:
                if i < len(column_names):
                    oracle_col_name = column_names[i]
                    original_col_name = reverse_mapping.get(oracle_col_name, oracle_col_name)
                    
                    # Get the expected Oracle type for this column
                    if original_col_name in df.columns:
                        polars_type = df[original_col_name].dtype
                        
                        # Apply recovery conversion based on type
                        if polars_type == pl.Utf8:
                            # For VARCHAR columns, always convert to string
                            converted_value = str(value) if value is not None else None
                        elif polars_type in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                                           pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                            # For integer columns
                            if value is None:
                                converted_value = None
                            elif isinstance(value, (int, float)):
                                converted_value = int(value)
                            else:
                                try:
                                    converted_value = int(float(str(value)))
                                except:
                                    converted_value = None
                        elif polars_type in [pl.Float32, pl.Float64]:
                            # For float columns
                            if value is None:
                                converted_value = None
                            elif isinstance(value, (int, float)):
                                converted_value = float(value)
                            else:
                                try:
                                    converted_value = float(str(value))
                                except:
                                    converted_value = None
                        elif isinstance(polars_type, (pl.Date, pl.Datetime)):
                            # For date/datetime columns
                            converted_value = self._convert_date_for_oracle(value)
                        else:
                            # Default: convert to string for safety
                            converted_value = str(value) if value is not None else None
                    else:
                        # Column not found, convert to string for safety
                        converted_value = str(value) if value is not None else None
                        
                    converted_row.append(converted_value)
                else:
                    # Shouldn't happen, but handle gracefully
                    converted_row.append(str(value) if value is not None else None)
                    
            except Exception as e:
                logger.debug(f"Error converting value {value} at position {i}: {e}")
                # Last resort: convert to string or None
                converted_row.append(str(value) if value is not None else None)
        
        return converted_row
    
    def insert_data(self, table_name: str, df: pl.DataFrame, 
                   column_mapping: Dict[str, str], batch_size: int = 1000) -> None:
        """
        Insert data into Oracle table using batch processing with error recovery.
        
        Args:
            table_name: Target table name
            df: Polars DataFrame with data
            column_mapping: Column name mapping
            batch_size: Number of rows to insert per batch
            
        Raises:
            oracledb.Error: If data insertion fails after retry attempts
        """
        try:
            oracle_conn = self.connection.get_connection()
            cursor = oracle_conn.cursor()
            
            # Prepare data
            column_names, data_rows = self.prepare_data_for_insert(df, column_mapping)
            
            # Create parameterized INSERT statement
            placeholders = ", ".join([":{}".format(i+1) for i in range(len(column_names))])
            columns_str = ", ".join(column_names)
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            logger.info(f"Inserting {len(data_rows)} rows into {table_name}")
            logger.debug(f"Insert SQL: {insert_sql}")
            
            # Insert data in batches with error recovery
            total_inserted = 0
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                
                try:
                    cursor.executemany(insert_sql, batch)
                    total_inserted += len(batch)
                    
                    if i + batch_size < len(data_rows):
                        logger.info(f"Inserted batch: {total_inserted}/{len(data_rows)} rows")
                        
                except Exception as batch_error:
                    logger.warning(f"Batch insertion failed at rows {i}-{i+len(batch)}: {batch_error}")
                    
                    # Try to recover by inserting rows one by one with type conversion
                    recovered_rows = self._insert_batch_with_recovery(
                        cursor, insert_sql, batch, column_names, df, column_mapping, i
                    )
                    total_inserted += recovered_rows
                    
                    logger.info(f"Recovered {recovered_rows}/{len(batch)} rows from failed batch")
            
            oracle_conn.commit()
            cursor.close()
            
            logger.info(f"Successfully inserted {total_inserted}/{len(data_rows)} rows into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {e}")
            oracle_conn.rollback()
            raise
    
    def execute_query(self, sql_query: str) -> pl.DataFrame:
        """
        Execute SQL query and return results as Polars DataFrame.
        
        Args:
            sql_query: SQL query string
            
        Returns:
            Polars DataFrame with query results
            
        Raises:
            oracledb.Error: If query execution fails
        """
        try:
            oracle_conn = self.connection.get_connection()
            cursor = oracle_conn.cursor()
            cursor.execute(sql_query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch data
            data = cursor.fetchall()
            cursor.close()
            
            # Convert to Polars DataFrame
            if data:
                # Convert to list of dictionaries
                records = [dict(zip(columns, row)) for row in data]
                df = pl.DataFrame(records)
            else:
                # Empty result
                df = pl.DataFrame({col: [] for col in columns})
            
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise