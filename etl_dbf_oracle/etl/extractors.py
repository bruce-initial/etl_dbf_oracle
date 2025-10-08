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
            # Use string schema by default to avoid mixed data type issues
            logger.info(f"Reading CSV {file_path} with string schema to avoid mixed type errors")

            # Force all columns to be read as strings initially
            string_schema_params = {
                'infer_schema_length': 0,  # Don't infer types, read everything as strings
                'try_parse_dates': False,  # We'll handle datetime parsing manually
                'null_values': ['', 'NULL', 'null', 'NA', 'N/A', 'n/a'],
                'ignore_errors': True,
                'truncate_ragged_lines': True,  # Handle inconsistent row lengths
            }

            # Merge with provided options, but prioritize our string schema approach
            if csv_options:
                # Only allow safe options that don't interfere with string schema
                safe_options = {k: v for k, v in csv_options.items()
                              if k not in ['infer_schema_length', 'try_parse_dates', 'schema_overrides']}
                string_schema_params.update(safe_options)

                # Log if we're overriding user options
                overridden_options = {k: v for k, v in csv_options.items()
                                    if k in ['infer_schema_length', 'try_parse_dates', 'schema_overrides']}
                if overridden_options:
                    logger.info(f"Overriding CSV options to use string schema: {overridden_options}")

            try:
                # Read with string schema
                df = pl.read_csv(file_path, **string_schema_params)
                logger.info(f"Successfully read CSV as strings: {len(df)} rows, {len(df.columns)} columns")

                # Log the column types to verify they're all strings
                logger.debug(f"Column types after string read: {df.dtypes}")

            except Exception as string_error:
                logger.error(f"String schema reading failed for {file_path}: {string_error}")
                logger.info("Attempting fallback with minimal parameters")

                # Ultra-minimal fallback
                minimal_params = {
                    'infer_schema_length': 0,
                    'ignore_errors': True,
                    'null_values': ['']
                }

                try:
                    df = pl.read_csv(file_path, **minimal_params)
                    logger.info(f"Minimal fallback successful: {len(df)} rows")
                except Exception as minimal_error:
                    logger.error(f"All CSV reading attempts failed: {minimal_error}")
                    raise minimal_error

            # Post-process datetime columns and handle type conversions
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
            # Check for memo file requirements first
            self._check_memo_files(file_path)

            # Open DBF table with encoding handling
            table = self._open_dbf_table(file_path, dbf_options)

            # Extract field information
            field_names, field_info = self._extract_field_info(table)

            # Process all records
            records = self._process_dbf_records(table, field_names)

            # Close the table
            table.close()

            # Convert to Polars DataFrame
            df = self._create_dataframe_from_records(records, field_names)

            # Post-process datetime columns
            df = self._fix_datetime_columns(df)

            logger.info(f"Successfully extracted DBF: {file_path} ({len(df)} rows)")
            return df

        except Exception as e:
            logger.error(f"Failed to extract DBF file {file_path}: {e}")
            raise

    def _get_dbf_encodings_to_try(self, dbf_options: dict = None) -> list:
        """
        Get list of encodings to try for DBF file opening.

        Args:
            dbf_options: DBF options that may contain forced encoding

        Returns:
            List of encodings to try in order
        """
        if dbf_options and 'encoding' in dbf_options:
            forced_encoding = dbf_options['encoding']
            logger.info(f"Using forced encoding from options: {forced_encoding}")
            return [forced_encoding]

        # Extended list of encodings to try, prioritizing cp1252 (Windows ANSI)
        return [
            'big5', 'cp437', # Surely work encoding
            'cp1252',  # Windows-1252 (Windows ANSI) - prioritized
            'windows-1252',  # Alternative name for cp1252
            'utf-8', 'latin1', 'iso-8859-1',
            'big5', 'gb2312', 'gbk', 'cp950', 'cp936',
            'cp1250', 'cp1251', 'cp1253', 'cp1254', 'cp1255',
            'cp1256', 'cp1257', 'cp1258', 'cp437', 'cp850', 'cp852', 'cp855',
            'cp857', 'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865',
            'cp866', 'cp869', 'cp874', 'windows-1250'
        ]

    def _open_dbf_table(self, file_path: str, dbf_options: dict = None):
        """
        Open DBF table with encoding handling.

        Args:
            file_path: Path to DBF file
            dbf_options: DBF options

        Returns:
            Opened DBF table object

        Raises:
            Exception: If unable to open DBF file with any encoding
        """
        encodings_to_try = self._get_dbf_encodings_to_try(dbf_options)
        table = None

        # Try each encoding
        for encoding in encodings_to_try:
            try:
                table = dbf.Table(file_path, codepage=encoding)
                table.open()
                logger.info(f"DBF opened with encoding: {encoding}")
                return table
            except (UnicodeDecodeError, dbf.DbfError, LookupError) as e:
                if table:
                    table.close()
                    table = None
                # Check for memo field specific errors
                if "memo field" in str(e).lower() or "table structure corrupt" in str(e).lower():
                    logger.warning(f"Memo field error with encoding {encoding}: {e}")
                continue

        # If forced encoding failed, try cp1252 fallback
        if dbf_options and 'encoding' in dbf_options:
            logger.warning(f"Forced encoding {dbf_options['encoding']} failed, trying cp1252 fallback")
            try:
                table = dbf.Table(file_path, codepage='cp1252')
                table.open()
                logger.info("DBF opened with cp1252 fallback encoding")
                return table
            except:
                pass

        # Final fallback without specifying encoding
        if not table:
            try:
                table = dbf.Table(file_path)
                table.open()
                logger.info("DBF opened with default encoding")
                return table
            except Exception as e:
                error_msg = str(e).lower()
                if "memo field" in error_msg or "table structure corrupt" in error_msg:
                    logger.warning(f"DBF file {file_path} contains memo fields - trying alternative reader")
                    table = self._read_without_memo_fields(file_path)
                    if table:
                        return table
                    raise Exception(f"Cannot read DBF file {file_path} - failed to skip memo fields")
                else:
                    raise Exception(f"Cannot open DBF file {file_path} with any encoding. Error: {e}")

    def _extract_field_info(self, table):
        """
        Extract field names and information from DBF table.

        Args:
            table: Opened DBF table object

        Returns:
            Tuple of (field_names, field_info_dict)
        """
        field_names = table.field_names
        field_info = {}

        try:
            # Get field definitions from table structure
            for field_def in table.structure():
                field_name = field_def[0]
                field_info[field_name] = {
                    'type': field_def[1],
                    'length': field_def[2] if len(field_def) > 2 else 0,
                    'decimal_count': field_def[3] if len(field_def) > 3 else 0
                }
        except:
            # Fallback: assume all fields are character type
            for field_name in field_names:
                field_info[field_name] = {
                    'type': 'C',
                    'length': 255,
                    'decimal_count': 0
                }

        return field_names, field_info

    def _process_dbf_records(self, table, field_names: list) -> list:
        """
        Process all records from DBF table.

        Args:
            table: Opened DBF table object
            field_names: List of field names

        Returns:
            List of record dictionaries
        """
        records = []

        for record_idx, record in enumerate(table):
            record_dict = self._process_single_record(record, field_names, record_idx)
            records.append(record_dict)

            # Log progress for large files
            if record_idx > 0 and record_idx % 1000 == 0:
                logger.info(f"Processed {record_idx} DBF records...")

        return records

    def _process_single_record(self, record, field_names: list, record_idx: int) -> dict:
        """
        Process a single DBF record and convert to dictionary.

        Args:
            record: DBF record object
            field_names: List of field names
            record_idx: Index of current record (for logging)

        Returns:
            Dictionary with processed field values
        """
        record_dict = {}

        for field_name in field_names:
            try:
                value = record[field_name]
                final_value = self._convert_dbf_value_to_string(value, field_name, record_idx)
                record_dict[field_name] = final_value

            except Exception as field_error:
                logger.error(f"Error processing field '{field_name}' in record {record_idx}: {field_error}")
                record_dict[field_name] = None

        return record_dict

    def _convert_dbf_value_to_string(self, value, field_name: str, record_idx: int):
        """
        Convert DBF field value to string with appropriate handling for different types.

        Args:
            value: Raw DBF field value
            field_name: Name of the field
            record_idx: Record index for logging

        Returns:
            Converted string value or None
        """
        if value is None:
            return None

        # Handle bytes values
        if isinstance(value, bytes):
            return self._decode_bytes_value(value)

        # Handle datetime objects
        if hasattr(value, 'date') and callable(getattr(value, 'date')):
            return self._convert_datetime_value(value)

        # Handle date objects
        if hasattr(value, '__class__') and 'date' in str(type(value)).lower():
            return value.isoformat() if hasattr(value, 'isoformat') else str(value)

        # Convert any other type to string with smart decimal formatting
        try:
            if isinstance(value, (int, float)):
                # Handle numeric values to remove unnecessary decimal places
                if isinstance(value, float):
                    # Check if the float has only zeros after decimal point
                    if value == int(value):
                        # No meaningful fractional part, format as integer
                        final_value = str(int(value))
                    else:
                        # Has meaningful fractional part, keep as is
                        final_value = str(value)
                else:
                    # It's already an integer
                    final_value = str(value)
            else:
                # For non-numeric values, convert to string normally
                str_value = str(value).strip() if value is not None else ""
                # Also handle string representations of numbers
                if str_value and self._is_numeric_string(str_value):
                    try:
                        float_val = float(str_value)
                        if float_val == int(float_val):
                            final_value = str(int(float_val))
                        else:
                            final_value = str_value
                    except ValueError:
                        final_value = str_value
                else:
                    final_value = str_value

            return None if final_value == "" else final_value
        except Exception as str_error:
            logger.warning(f"Failed to convert value to string for field '{field_name}' in record {record_idx}: {str_error}")
            return str(value)[:100]  # Truncate if conversion fails

    def _is_numeric_string(self, value: str) -> bool:
        """
        Check if a string represents a numeric value.

        Args:
            value: String to check

        Returns:
            True if string represents a number
        """
        try:
            float(value)
            return True
        except ValueError:
            return False

    def _decode_bytes_value(self, value: bytes) -> str:
        """
        Decode bytes value trying multiple encodings.

        Args:
            value: Bytes value to decode

        Returns:
            Decoded string value
        """
        field_encodings = [
            'cp1252', 'windows-1252', 'utf-8', 'latin1', 'iso-8859-1'
        ]

        for encoding in field_encodings:
            try:
                decoded_value = value.decode(encoding).strip()
                return str(decoded_value) if decoded_value else ""
            except (UnicodeDecodeError, LookupError):
                continue

        # Fallback with error replacement
        decoded_value = value.decode('utf-8', errors='replace').strip()
        return str(decoded_value) if decoded_value else ""

    def _convert_datetime_value(self, value) -> str:
        """
        Convert datetime value to mm/dd/yyyy hh:mm AMPM format string.

        Args:
            value: Datetime value

        Returns:
            Formatted datetime string in mm/dd/yyyy hh:mm AMPM format
        """
        try:
            import datetime
            
            # Convert the value to a datetime object if it isn't already
            if hasattr(value, 'time') and callable(getattr(value, 'time')):
                # Full datetime object
                dt = value
            elif hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
                # Date or datetime object
                if hasattr(value, 'hour'):
                    # It's a datetime object with time
                    dt = value
                else:
                    # It's a date-only object - format as mm/dd/yyyy only
                    return value.strftime('%m/%d/%Y')
            else:
                # Try to parse if it's a string or other format
                if isinstance(value, str):
                    # Try to parse common datetime string formats
                    datetime_formats = ['%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S']
                    date_only_formats = ['%Y-%m-%d', '%m/%d/%Y']
                    
                    # Try datetime formats first
                    for fmt in datetime_formats:
                        try:
                            dt = datetime.datetime.strptime(value, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        # Try date-only formats
                        for fmt in date_only_formats:
                            try:
                                date_obj = datetime.datetime.strptime(value, fmt).date()
                                return date_obj.strftime('%m/%d/%Y')
                            except ValueError:
                                continue
                        else:
                            # If no format worked, return the original string
                            return str(value)
                else:
                    return str(value)

            # Format as mm/dd/yyyy hh:mm AMPM
            return dt.strftime('%m/%d/%Y %I:%M %p')
            
        except Exception as e:
            logger.warning(f"Failed to format datetime value {value}: {e}")
            return str(value)

    def _create_dataframe_from_records(self, records: list, field_names: list) -> pl.DataFrame:
        """
        Create Polars DataFrame from processed records.

        Args:
            records: List of record dictionaries
            field_names: List of field names for empty DataFrame

        Returns:
            Polars DataFrame
        """
        if not records:
            return pl.DataFrame({field: [] for field in field_names})

        logger.info(f"Creating DataFrame from {len(records)} DBF records using safe creation method")

        try:
            # Import safe DataFrame creator
            from ..utils.safe_dataframe import safe_records_to_dataframe

            # Use the safe creation method that handles all edge cases
            df = safe_records_to_dataframe(records)
            logger.info(f"✅ Successfully created Polars DataFrame using safe method: {len(df)} rows, {len(df.columns)} columns")

            # Verify column types
            non_string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype != pl.Utf8]
            if non_string_cols:
                logger.warning(f"Some columns are not strings: {non_string_cols}")
            else:
                logger.info("✅ All columns are strings as expected")

            return df

        except Exception as safe_error:
            logger.error(f"Safe DataFrame creation failed: {safe_error}")
            return self._fallback_dataframe_creation(records)

    def _fallback_dataframe_creation(self, records: list) -> pl.DataFrame:
        """
        Fallback method for DataFrame creation using pandas.

        Args:
            records: List of record dictionaries

        Returns:
            Polars DataFrame

        Raises:
            Exception: If all DataFrame creation methods fail
        """
        logger.info("Falling back to pandas-based conversion with aggressive string conversion")

        try:
            import pandas as pd

            # Convert records to pandas with string conversion
            logger.info("Converting all values to strings before pandas DataFrame creation")
            string_records = []
            for record in records:
                string_record = {}
                for key, value in record.items():
                    if value is None:
                        string_record[key] = None
                    else:
                        try:
                            string_record[key] = str(value)
                        except Exception as str_error:
                            logger.warning(f"Failed to convert value to string for key '{key}': {str_error}")
                            string_record[key] = str(value)[:100]  # Truncate problematic values
                string_records.append(string_record)

            # Create pandas DataFrame
            pandas_df = pd.DataFrame(string_records, dtype=str)

            # Clean up pandas DataFrame
            for col in pandas_df.columns:
                pandas_df[col] = pandas_df[col].replace({
                    'nan': None, 'NaN': None, '<NA>': None,
                    'None': None, 'null': None, 'NULL': None
                })

            # Convert to Polars with safe method
            from ..utils.safe_dataframe import safe_from_pandas
            df = safe_from_pandas(pandas_df, force_string_schema=True)

            logger.info(f"✅ Fallback method successful: {len(df)} rows, types: {df.dtypes}")
            return df

        except Exception as pandas_fallback_error:
            logger.error(f"All DBF DataFrame creation methods failed: {pandas_fallback_error}")
            raise pandas_fallback_error

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
        Also handles mixed data types by ensuring consistent column types.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with properly parsed datetime columns and consistent types
        """
        transformations = []
        datetime_patterns = [
            "%Y-%m-%d %H:%M:%S%.f",  # 2025-07-23 17:56:23.001 (correct format)
            "%Y-%m-%d %H:%M:%S",     # 2025-07-23 17:56:23
            "%Y-%m-%d",              # 2025-07-23 (like the error example: '2005-01-07')
            "%m/%d/%Y %H:%M:%S%.f",  # 07/23/2025 17:56:23.001
            "%m/%d/%Y %H:%M:%S",     # 07/23/2025 17:56:23
            "%m/%d/%Y",              # 07/23/2025
            "%d/%m/%Y",              # 07/23/2025 (European format)
            "%Y%m%d",                # 20250723 (compact format)
        ]

        for col in df.columns:
            col_data = df[col]

            # First, handle mixed data type columns by converting everything to strings
            if col_data.dtype != pl.Utf8:
                try:
                    # Convert non-string columns to strings to handle mixed types
                    transformations.append(pl.col(col).cast(pl.Utf8, strict=False))
                    col_data = df.select(pl.col(col).cast(pl.Utf8, strict=False)).to_series()
                    logger.debug(f"Converted column '{col}' from {df[col].dtype} to string to handle mixed types")
                except Exception as e:
                    logger.warning(f"Could not convert column '{col}' to string: {e}")
                    transformations.append(pl.col(col))
                    continue

            # Check if column contains datetime-like strings or has datetime-like name
            is_datetime_column = False

            # Check by column name patterns
            col_lower = col.strip().lower()
            if any(pattern in col_lower for pattern in ['date', 'time', '_at', '_on', 'created', 'updated', 'modified']):
                is_datetime_column = True

            # Check by content patterns (sample first few non-null values)
            if not is_datetime_column and col_data.dtype == pl.Utf8:
                sample_values = col_data.drop_nulls().head(10).to_list()
                for val in sample_values:
                    if val and isinstance(val, str):
                        # Look for date-like patterns in the data
                        val_clean = val.strip()
                        if (len(val_clean) >= 8 and
                            any(char.isdigit() for char in val_clean) and
                            any(sep in val_clean for sep in ['-', '/', ' ', ':'])):
                            is_datetime_column = True
                            break

            if is_datetime_column and col_data.dtype == pl.Utf8 and not col_data.is_null().all():
                # Try parsing with different patterns and format to custom format
                formatted_col = None
                for pattern in datetime_patterns:
                    try:
                        # First, parse to datetime to validate the pattern works
                        test_parsed = (pl.col(col)
                                     .str.strip_chars()
                                     .str.strptime(pl.Datetime, pattern, strict=False))

                        # Test if parsing works by applying to a sample
                        test_df = df.select(test_parsed.alias("test"))
                        non_null_count = test_df.filter(pl.col("test").is_not_null()).height

                        if non_null_count > 0:
                            logger.info(f"Successfully parsed datetime column '{col}' with pattern '{pattern}' ({non_null_count} valid dates)")
                            
                            # Now format according to our custom format requirements
                            if pattern in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]:
                                # Date-only patterns - format as mm/dd/yyyy
                                formatted_col = (pl.col(col)
                                               .str.strip_chars()
                                               .str.strptime(pl.Datetime, pattern, strict=False)
                                               .dt.strftime("%m/%d/%Y"))
                            else:
                                # DateTime patterns - format as mm/dd/yyyy hh:mm AMPM
                                formatted_col = (pl.col(col)
                                               .str.strip_chars()
                                               .str.strptime(pl.Datetime, pattern, strict=False)
                                               .dt.strftime("%m/%d/%Y %I:%M %p"))
                            
                            transformations.append(formatted_col.alias(col))
                            break
                    except Exception as e:
                        logger.debug(f"Pattern '{pattern}' failed for column '{col}': {e}")
                        continue

                if formatted_col is None:
                    logger.warning(f"Could not parse datetime column '{col}', keeping as string")
                    # Ensure it's still a clean string column
                    transformations.append(pl.col(col).cast(pl.Utf8, strict=False))
            else:
                # For non-datetime columns, ensure consistent string type if we detected mixed types
                if col in [t.meta.root_names()[0] for t in transformations if hasattr(t.meta, 'root_names')]:
                    # Already handled above
                    continue
                else:
                    transformations.append(pl.col(col))

        if len(transformations) > 0:
            try:
                return df.with_columns(transformations)
            except Exception as e:
                logger.warning(f"Failed to apply column transformations: {e}")
                logger.info("Falling back to original DataFrame with all columns as strings")
                # Last resort: convert everything to strings
                string_transformations = [pl.col(col).cast(pl.Utf8, strict=False) for col in df.columns]
                return df.with_columns(string_transformations)

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
                return self.extract_from_multiple_csv(file_path, config.csv_options,
                                                    getattr(config, '_etl_callback', None), config)
            else:
                return self.extract_from_csv(file_path, config.csv_options)

        elif source_type == 'dbf':
            if not file_path:
                raise ValueError("file_path is required for DBF source type")
            if isinstance(file_path, list):
                return self.extract_from_multiple_dbf(file_path, config.dbf_options,
                                                    getattr(config, '_etl_callback', None), config)
            else:
                return self.extract_from_dbf(file_path, config.dbf_options)

        elif source_type == 'xlsx':
            if not file_path:
                raise ValueError("file_path is required for XLSX source type")
            if isinstance(file_path, list):
                return self.extract_from_multiple_xlsx(file_path, config.xlsx_options,
                                                     getattr(config, '_etl_callback', None), config)
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
                        # Use safe DataFrame creation
                        try:
                            from ..utils.safe_dataframe import safe_records_to_dataframe
                            result_df = safe_records_to_dataframe(records)
                        except Exception as safe_error:
                            logger.warning(f"Safe DataFrame creation failed: {safe_error}")
                            result_df = pl.DataFrame(records)
                    else:
                        string_schema = {col: pl.Utf8 for col in columns}
                        result_df = pl.DataFrame({col: [] for col in columns}, schema=string_schema)

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

            # Convert back to Polars DataFrame using safe creation
            if result:
                records = [dict(zip(columns, row)) for row in result]
                # Use safe DataFrame creation
                try:
                    from ..utils.safe_dataframe import safe_records_to_dataframe
                    result_df = safe_records_to_dataframe(records)
                except Exception as safe_error:
                    logger.warning(f"Safe DataFrame creation failed: {safe_error}")
                    result_df = pl.DataFrame(records)
            else:
                string_schema = {col: pl.Utf8 for col in columns}
                result_df = pl.DataFrame({col: [] for col in columns}, schema=string_schema)

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

    def extract_from_multiple_csv(self, file_paths: list, csv_options: dict = None,
                                 etl_callback=None, config=None) -> pl.DataFrame:
        """
        Extract and process CSV files one by one to reduce memory usage.

        Args:
            file_paths: List of CSV file paths
            csv_options: Additional options for CSV reading
            etl_callback: Function to call for each file's ETL processing
            config: TableConfig for ETL processing

        Returns:
            Combined Polars DataFrame if no callback, else None
        """
        try:
            logger.info(f"Starting file-by-file processing: {len(file_paths)} CSV files")

            if etl_callback and config:
                # Process each file individually with full ETL
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: ETL processing batch {batch_num}")

                    # Extract data and add source file column before ETL
                    df = self.extract_from_csv(file_path, csv_options)
                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))

                    # Run ETL with the modified DataFrame
                    etl_callback(config, df)
                    logger.info(f"Status: Batch {batch_num} ETL complete")

                return None  # No combined DataFrame needed
            else:
                # Fallback to original behavior for backward compatibility
                dataframes = []
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: Extracting batch {batch_num}")
                    df = self.extract_from_csv(file_path, csv_options)
                    logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")

                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                    dataframes.append(df)

                combined_df = pl.concat(dataframes, how="diagonal_relaxed") if len(dataframes) > 1 else dataframes[0]
                logger.info(f"Batch processing complete: {len(file_paths)} CSV files, {len(combined_df)} total rows")
                return combined_df

        except Exception as e:
            logger.error(f"Failed to process multiple CSV files: {e}")
            raise

    def extract_from_multiple_dbf(self, file_paths: list, dbf_options: dict = None,
                                 etl_callback=None, config=None) -> pl.DataFrame:
        """
        Extract and process DBF files one by one to reduce memory usage.

        Args:
            file_paths: List of DBF file paths
            dbf_options: Additional options for DBF reading
            etl_callback: Function to call for each file's ETL processing
            config: TableConfig for ETL processing

        Returns:
            Combined Polars DataFrame if no callback, else None
        """
        try:
            logger.info(f"Starting file-by-file processing: {len(file_paths)} DBF files")

            if etl_callback and config:
                # Process each file individually with full ETL
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: ETL processing batch {batch_num}")

                    # Extract data and add source file column before ETL
                    df = self.extract_from_dbf(file_path, dbf_options)
                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))

                    # Run ETL with the modified DataFrame
                    etl_callback(config, df)
                    logger.info(f"Status: Batch {batch_num} ETL complete")

                return None  # No combined DataFrame needed
            else:
                # Fallback to original behavior for backward compatibility
                dataframes = []
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: Extracting batch {batch_num}")
                    df = self.extract_from_dbf(file_path, dbf_options)
                    logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")

                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                    dataframes.append(df)

                combined_df = pl.concat(dataframes, how="diagonal_relaxed") if len(dataframes) > 1 else dataframes[0]
                logger.info(f"Batch processing complete: {len(file_paths)} DBF files, {len(combined_df)} total rows")
                return combined_df

        except Exception as e:
            logger.error(f"Failed to process multiple DBF files: {e}")
            raise

    def extract_from_multiple_xlsx(self, file_paths: list, xlsx_options: dict = None,
                                  etl_callback=None, config=None) -> pl.DataFrame:
        """
        Extract and process XLSX files one by one to reduce memory usage.

        Args:
            file_paths: List of XLSX file paths
            xlsx_options: Additional options for XLSX reading
            etl_callback: Function to call for each file's ETL processing
            config: TableConfig for ETL processing

        Returns:
            Combined Polars DataFrame if no callback, else None
        """
        try:
            logger.info(f"Starting file-by-file processing: {len(file_paths)} XLSX files")

            if etl_callback and config:
                # Process each file individually with full ETL
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: ETL processing batch {batch_num}")

                    # Extract data and add source file column before ETL
                    df = self.extract_from_xlsx(file_path, xlsx_options)
                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))

                    # Run ETL with the modified DataFrame
                    etl_callback(config, df)
                    logger.info(f"Status: Batch {batch_num} ETL complete")

                return None  # No combined DataFrame needed
            else:
                # Fallback to original behavior for backward compatibility
                dataframes = []
                for i, file_path in enumerate(file_paths):
                    batch_num = i+1
                    logger.info(f"Processing batch {batch_num}/{len(file_paths)}: {file_path}")
                    logger.info(f"Status: Extracting batch {batch_num}")
                    df = self.extract_from_xlsx(file_path, xlsx_options)
                    logger.info(f"Status: Batch {batch_num} extracted ({len(df)} rows)")

                    df = df.with_columns(pl.lit(file_path).alias("_source_file"))
                    dataframes.append(df)

                combined_df = pl.concat(dataframes, how="diagonal_relaxed") if len(dataframes) > 1 else dataframes[0]
                logger.info(f"Batch processing complete: {len(file_paths)} XLSX files, {len(combined_df)} total rows")
                return combined_df

        except Exception as e:
            logger.error(f"Failed to process multiple XLSX files: {e}")
            raise

    def _read_csv_line_by_line(self, file_path: str) -> pl.DataFrame:
        """
        Last resort method to read CSV line by line when all other methods fail.

        Args:
            file_path: Path to CSV file

        Returns:
            Polars DataFrame with all columns as strings
        """
        logger.info(f"Reading CSV line-by-line: {file_path}")

        import csv
        rows = []
        headers = None

        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Use Python's CSV reader which is very tolerant
            csv_reader = csv.reader(f)

            for i, row in enumerate(csv_reader):
                if i == 0:
                    headers = row
                    logger.debug(f"Headers: {headers}")
                else:
                    # Ensure row has same number of columns as headers
                    while len(row) < len(headers):
                        row.append('')  # Pad with empty strings
                    if len(row) > len(headers):
                        row = row[:len(headers)]  # Truncate excess columns

                    rows.append(row)

        # Create dictionary for DataFrame
        data_dict = {}
        for i, header in enumerate(headers):
            data_dict[header] = [row[i] if i < len(row) else '' for row in rows]

        # Create Polars DataFrame with explicit string schema
        string_schema = {col: pl.Utf8 for col in headers}
        df = pl.DataFrame(data_dict, schema=string_schema)

        logger.info(f"Line-by-line reading successful: {len(df)} rows, {len(df.columns)} columns")
        return df

    def _check_memo_files(self, dbf_file_path: str) -> None:
        """
        Check if DBF file requires memo files and validate their existence.

        Args:
            dbf_file_path: Path to the DBF file
        """
        try:
            dbf_path = Path(dbf_file_path)
            base_name = dbf_path.stem
            parent_dir = dbf_path.parent

            # Common memo file extensions
            memo_extensions = ['.dbt', '.DBT', '.fpt', '.FPT']

            # Check if any memo files exist
            existing_memo_files = []
            for ext in memo_extensions:
                memo_file = parent_dir / f"{base_name}{ext}"
                if memo_file.exists():
                    existing_memo_files.append(str(memo_file))

            if existing_memo_files:
                logger.info(f"Found memo files: {existing_memo_files}")
            else:
                logger.debug(f"No memo files found for {dbf_file_path}")

        except Exception as e:
            logger.warning(f"Failed to check memo files for {dbf_file_path}: {e}")

    def _get_memo_file_suggestion(self, dbf_file_path: str) -> str:
        """
        Get helpful suggestions for resolving memo file issues.

        Args:
            dbf_file_path: Path to the DBF file

        Returns:
            Suggestion message for resolving memo file issues
        """
        try:
            dbf_path = Path(dbf_file_path)
            base_name = dbf_path.stem
            parent_dir = dbf_path.parent

            # Look for potential memo files in the same directory
            all_files = list(parent_dir.glob(f"{base_name}.*"))
            memo_candidates = [f for f in all_files if f.suffix.lower() in ['.dbt', '.fpt']]

            suggestions = []
            suggestions.append(f"Expected memo file: {base_name}.dbt or {base_name}.fpt")

            if memo_candidates:
                suggestions.append(f"Found potential memo files: {[str(f) for f in memo_candidates]}")
            else:
                suggestions.append("No memo files found in the same directory.")
                # List all files in directory for debugging
                all_files_str = [f.name for f in all_files]
                suggestions.append(f"Available files: {all_files_str}")

            suggestions.append("Solution: Ensure the corresponding .dbt or .fpt file is in the same directory as the .dbf file.")

            return " ".join(suggestions)

        except Exception as e:
            return f"Please ensure the corresponding memo file (.dbt or .fpt) is in the same directory. Error: {e}"

    def _read_without_memo_fields(self, file_path: str):
        """Read DBF file skipping memo fields using simpledbf."""
        try:
            import pandas as pd
            from simpledbf import Dbf5

            # Use simpledbf which can handle memo field issues better
            dbf_obj = Dbf5(file_path, codec='cp1252')
            df = dbf_obj.to_dataframe()

            # Convert to polars
            import polars as pl
            polars_df = pl.from_pandas(df)

            # Create mock table object
            class MockTable:
                def __init__(self, df):
                    self.field_names = df.columns
                    self._df = df

                def __iter__(self):
                    return iter(self._df.to_dicts())

                def close(self):
                    pass

                def structure(self):
                    return [(col, 'C', 255, 0) for col in self.field_names]

            logger.info(f"Successfully read DBF with simpledbf, skipping memo fields")
            return MockTable(polars_df)

        except ImportError:
            logger.warning("simpledbf not available - install with: pip install simpledbf")
            return None
        except Exception as e:
            logger.debug(f"simpledbf reading failed: {e}")
            return None
