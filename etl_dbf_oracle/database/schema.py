"""
Database schema management and DDL generation.
"""

import polars as pl
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class SchemaManager:
    """Handles Oracle schema operations and DDL generation."""
    
    @staticmethod
    def polars_to_oracle_type(polars_type: pl.DataType) -> str:
        """
        Map Polars data types to Oracle data types.
        
        Args:
            polars_type: Polars data type
            
        Returns:
            Oracle data type string
        """
        # Handle specific type instances first
        if isinstance(polars_type, pl.Datetime):
            return "TIMESTAMP"
        elif isinstance(polars_type, pl.Date):
            return "DATE"
        elif isinstance(polars_type, pl.Time):
            return "TIMESTAMP"
        elif isinstance(polars_type, pl.Duration):
            return "INTERVAL DAY TO SECOND"
        elif isinstance(polars_type, pl.Utf8):
            return "VARCHAR2(4000)"  # Default size, can be optimized based on data
        
        # Handle simple type mappings
        type_mapping = {
            pl.Int8: "NUMBER(3)",
            pl.Int16: "NUMBER(5)",
            pl.Int32: "NUMBER(10)",
            pl.Int64: "NUMBER(19)",
            pl.UInt8: "NUMBER(3)",
            pl.UInt16: "NUMBER(5)",
            pl.UInt32: "NUMBER(10)",
            pl.UInt64: "NUMBER(20)",
            pl.Float32: "BINARY_FLOAT",
            pl.Float64: "BINARY_DOUBLE",
            pl.Boolean: "NUMBER(1)",
        }
        
        return type_mapping.get(polars_type, "VARCHAR2(4000)")
    
    @staticmethod
    def analyze_column_sizes(df: pl.DataFrame) -> Dict[str, int]:
        """
        Analyze string columns to determine optimal VARCHAR2 sizes.
        
        Args:
            df: Polars DataFrame
            
        Returns:
            Dictionary with column names and their max string lengths
        """
        column_sizes = {}
        
        for col in df.columns:
            if df[col].dtype == pl.Utf8:
                # Get max length, handling nulls
                max_length = df.select(
                    pl.col(col).str.len_chars().max().alias("max_len")
                )["max_len"][0]
                
                if max_length is None:
                    max_length = 255  # Default for all null columns
                else:
                    # Add substantial buffer for safety and ensure minimum size
                    # Use a more generous buffer to prevent truncation errors
                    if max_length <= 50:
                        max_length = 255  # Small columns get reasonable default
                    elif max_length <= 100:
                        max_length = max_length * 2  # Double for medium columns
                    elif max_length <= 500:
                        max_length = max_length * 1.5  # 50% buffer for larger columns
                    else:
                        max_length = max_length * 1.2  # 20% buffer for very large columns
                    
                    # Ensure reasonable bounds
                    max_length = max(max_length, 100)  # Minimum 100 chars
                    max_length = min(max_length, 4000)  # Oracle VARCHAR2 limit
                
                column_sizes[col] = int(max_length)
        
        logger.debug(f"Analyzed column sizes for {len(column_sizes)} string columns")
        return column_sizes
    
    @classmethod
    def create_table_ddl(cls, table_name: str, df: pl.DataFrame, 
                        column_mapping: Dict[str, str]) -> str:
        """
        Generate CREATE TABLE DDL statement.
        
        Args:
            table_name: Name of the table to create
            df: Polars DataFrame with the data
            column_mapping: Mapping of original to sanitized column names
            
        Returns:
            DDL statement string
        """
        column_sizes = cls.analyze_column_sizes(df)
        
        ddl_parts = [f"CREATE TABLE {table_name} ("]
        column_definitions = []
        
        for original_col, sanitized_col in column_mapping.items():
            polars_type = df[original_col].dtype
            
            if polars_type == pl.Utf8 and original_col in column_sizes:
                oracle_type = f"VARCHAR2({column_sizes[original_col]})"
            else:
                oracle_type = cls.polars_to_oracle_type(polars_type)
            
            column_definitions.append(f"    {sanitized_col} {oracle_type}")
        
        ddl_parts.append(",\n".join(column_definitions))
        ddl_parts.append(")")
        
        ddl_statement = "\n".join(ddl_parts)
        logger.debug(f"Generated DDL for table {table_name}:")
        logger.debug(ddl_statement)
        
        # Also log the column type mappings for debugging
        type_info = []
        for original_col, sanitized_col in column_mapping.items():
            polars_type = df[original_col].dtype
            if polars_type == pl.Utf8 and original_col in column_sizes:
                oracle_type = f"VARCHAR2({column_sizes[original_col]})"
            else:
                oracle_type = cls.polars_to_oracle_type(polars_type)
            type_info.append(f"{original_col} ({polars_type}) -> {sanitized_col} ({oracle_type})")
        
        logger.debug(f"Column type mappings: {'; '.join(type_info)}")
        return ddl_statement
    
    @staticmethod
    def create_primary_key_ddl(table_name: str, primary_key_columns: List[str]) -> str:
        """
        Generate primary key constraint DDL.
        
        Args:
            table_name: Target table name
            primary_key_columns: List of primary key column names
            
        Returns:
            Primary key DDL statement
        """
        if not primary_key_columns:
            return ""
        
        pk_columns = ", ".join(primary_key_columns)
        constraint_name = f"PK_{table_name}"
        
        return f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({pk_columns})"
    
    @staticmethod
    def create_foreign_key_ddl(table_name: str, foreign_key: Dict[str, any], index: int) -> str:
        """
        Generate foreign key constraint DDL.
        
        Args:
            table_name: Target table name
            foreign_key: Foreign key definition dictionary
            index: Index for constraint naming
            
        Returns:
            Foreign key DDL statement
        """
        fk_columns = ", ".join(foreign_key['columns'])
        ref_columns = ", ".join(foreign_key['reference_columns'])
        ref_table = foreign_key['reference_table']
        constraint_name = f"FK_{table_name}_{index+1}"
        
        return f"""ALTER TABLE {table_name} 
                 ADD CONSTRAINT {constraint_name} 
                 FOREIGN KEY ({fk_columns}) 
                 REFERENCES {ref_table} ({ref_columns})"""
    
    @staticmethod
    def create_index_ddl(table_name: str, index_def: Dict[str, any]) -> str:
        """
        Generate index creation DDL.
        
        Args:
            table_name: Target table name
            index_def: Index definition dictionary
            
        Returns:
            Index DDL statement
        """
        idx_name = index_def.get('name', f"IDX_{table_name}_{len(index_def['columns'])}")
        idx_columns = ", ".join(index_def['columns'])
        unique_keyword = "UNIQUE " if index_def.get('unique', False) else ""
        
        return f"CREATE {unique_keyword}INDEX {idx_name} ON {table_name} ({idx_columns})"