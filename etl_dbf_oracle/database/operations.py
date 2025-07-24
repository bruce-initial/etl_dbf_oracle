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
        Prepare data for Oracle insertion with proper type conversion.
        
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
        
        # Convert to list of lists for Oracle insertion
        # Handle None/null values and type conversion properly
        data_rows = []
        for row in df_renamed.iter_rows():
            processed_row = []
            for i, (value, col_name) in enumerate(zip(row, df_renamed.columns)):
                if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
                    processed_row.append(None)
                else:
                    # Convert value based on expected Oracle type
                    col_type = column_types[col_name]
                    processed_value = self._convert_value_for_oracle(value, col_type)
                    processed_row.append(processed_value)
            data_rows.append(processed_row)
        
        column_names = list(column_mapping.values())
        
        logger.debug(f"Prepared {len(data_rows)} rows for insertion")
        return column_names, data_rows
    
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
                if isinstance(value, str):
                    return value  # Let Oracle handle date parsing
                return str(value)
            
            # Default: convert to string
            else:
                return str(value)
                
        except Exception as e:
            logger.warning(f"Failed to convert value {value} of type {type(value)} for Polars type {polars_type}: {e}")
            return str(value)  # Fallback to string
    
    def insert_data(self, table_name: str, df: pl.DataFrame, 
                   column_mapping: Dict[str, str], batch_size: int = 1000) -> None:
        """
        Insert data into Oracle table using batch processing.
        
        Args:
            table_name: Target table name
            df: Polars DataFrame with data
            column_mapping: Column name mapping
            batch_size: Number of rows to insert per batch
            
        Raises:
            oracledb.Error: If data insertion fails
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
            
            # Insert data in batches
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                
                if i + batch_size < len(data_rows):
                    logger.info(f"Inserted batch: {i + len(batch)}/{len(data_rows)} rows")
            
            oracle_conn.commit()
            cursor.close()
            
            logger.info(f"Successfully inserted all {len(data_rows)} rows into {table_name}")
            
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