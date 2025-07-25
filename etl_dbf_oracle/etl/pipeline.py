"""
Main ETL pipeline orchestration.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from ..config.table_config import TableConfig
from ..database.connection import OracleConnection
from ..database.operations import DatabaseOperations
from .extractors import DataExtractor
from .transformers import DataTransformer
from .loaders import DataLoader

logger = logging.getLogger(__name__)


class OracleETL:
    """
    Main ETL pipeline for reading data from various sources 
    and loading into Oracle database with automatic schema management.
    """
    
    def __init__(self, connection: OracleConnection):
        """
        Initialize Oracle ETL pipeline.
        
        Args:
            connection: Oracle connection instance
        """
        self.connection = connection
        self.db_operations = DatabaseOperations(connection)
        self.extractor = DataExtractor(self.db_operations)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.db_operations)
    
    @classmethod
    def from_env_file(cls, env_file: str = '.env') -> 'OracleETL':
        """
        Create OracleETL instance from environment file.
        
        Args:
            env_file: Path to .env file (default: '.env')
            
        Returns:
            OracleETL instance with connection parameters from .env
            
        Raises:
            ValueError: If required environment variables are missing
        """
        connection = OracleConnection.from_env_file(env_file)
        return cls(connection)
    
    def run_etl_for_table(self, config: TableConfig, file_path: Optional[str] = None,
                         transformation_options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run ETL pipeline for a single table configuration.
        
        Args:
            config: TableConfig object with all settings
            file_path: Path to source file (required for CSV sources)
            transformation_options: Options for data transformation
            
        Returns:
            Dictionary with ETL results and statistics
            
        Raises:
            Exception: If any step of ETL pipeline fails
        """
        etl_results = {
            'table_name': config.target_table,
            'source_type': config.source_type,
            'extraction_success': False,
            'transformation_success': False,
            'loading_success': False,
            'total_rows': 0,
            'column_mapping': {},
            'load_info': {}
        }
        
        try:
            logger.info(f"Starting ETL pipeline for table: {config.target_table}")
            logger.info(f"Source type: {config.source_type}")
            
            # Ensure connection is established
            if not self.connection.is_connected():
                self.connection.connect()
            
            # Step 1: Extract data
            logger.info("Step 1: Extracting data...")
            df = self.extractor.extract_data(config, file_path)
            self.extractor.validate_extracted_data(df, config)
            etl_results['extraction_success'] = True
            etl_results['total_rows'] = len(df)
            logger.info(f"Extracted {len(df)} rows with {len(df.columns)} columns")
            
            # Step 2: Transform data
            logger.info("Step 2: Transforming data...")
            transform_options = transformation_options or {}
            transformed_df, column_mapping = self.transformer.transform_data(df, **transform_options)
            etl_results['transformation_success'] = True
            if column_mapping:
                etl_results['column_mapping'] = column_mapping
            logger.info(f"Transformed data: {len(transformed_df)} rows with {len(transformed_df.columns)} columns")
            
            # Step 3: Load data
            logger.info("Step 3: Loading data...")
            
            # Create table/file structure based on target type
            if config.target_type.lower() == 'oracle':
                # Create Oracle table structure
                if not column_mapping:
                    # If no column mapping from transformation, create one for loading
                    from ..utils.helpers import ColumnSanitizer
                    column_mapping = ColumnSanitizer.sanitize_column_names(transformed_df.columns)
                
                table_column_mapping = self.loader.create_table_structure(config, transformed_df)
            elif config.target_type.lower() == 'dbf':
                # Create DBF file structure
                if not column_mapping:
                    # If no column mapping from transformation, create one for loading
                    from ..utils.helpers import ColumnSanitizer
                    column_mapping = ColumnSanitizer.sanitize_column_names(transformed_df.columns)
                
                table_column_mapping = self.loader.create_dbf_structure(config, transformed_df)
            elif config.target_type.lower() == 'xlsx':
                # Create XLSX file structure
                if not column_mapping:
                    # If no column mapping from transformation, create one for loading
                    from ..utils.helpers import ColumnSanitizer
                    column_mapping = ColumnSanitizer.sanitize_column_names(transformed_df.columns)
                
                table_column_mapping = self.loader.create_xlsx_structure(config, transformed_df)
            else:
                raise ValueError(f"Unsupported target type: {config.target_type}")
            
            # Load data with proper column mapping
            load_info = self.loader.load_data(config, transformed_df, table_column_mapping)
            etl_results['loading_success'] = True
            etl_results['load_info'] = load_info
            
            logger.info(f"ETL pipeline completed successfully for {config.target_table}!")
            logger.info(f"Final column mapping: {table_column_mapping}")
            
            return etl_results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed for {config.target_table}: {e}")
            etl_results['error'] = str(e)
            raise
    
    def run_etl_with_config(self, config: TableConfig, file_path: Optional[str] = None) -> None:
        """
        Run ETL pipeline using table configuration (backward compatibility).
        
        Args:
            config: TableConfig object with all settings
            file_path: Path to source file (required for CSV sources)
            
        Raises:
            Exception: If ETL pipeline fails
        """
        try:
            self.run_etl_for_table(config, file_path)
        finally:
            # Always disconnect when using this method for backward compatibility
            if self.connection.is_connected():
                self.connection.disconnect()
    
    def run_multiple_tables(self, config_file: str, data_directory: str = "data/",
                           transformation_options: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Run ETL for multiple tables defined in configuration file.
        
        Args:
            config_file: Path to YAML configuration file
            data_directory: Directory containing CSV files (if applicable)
            transformation_options: Options for data transformation
            
        Returns:
            Dictionary with results for each table
            
        Raises:
            Exception: If configuration loading fails
        """
        all_results = {}
        
        try:
            # Load table configurations
            table_configs = TableConfig.load_from_yaml(config_file)
            logger.info(f"Loaded configuration for {len(table_configs)} tables")
            
            # Ensure connection is established
            if not self.connection.is_connected():
                self.connection.connect()
            
            # Process each table
            for table_name, config in table_configs.items():
                logger.info(f"Processing table: {table_name}")
                
                try:
                    # Get explicit file path from configuration for file-based sources
                    file_path = None
                    if config.source_type.lower() in ['csv', 'dbf', 'xlsx']:
                        if hasattr(config, 'source_file_path') and config.source_file_path:
                            raw_file_path = config.source_file_path
                            
                            # Handle both single files and multiple files
                            if isinstance(raw_file_path, list):
                                # Multiple files - process each one
                                file_path = []
                                missing_files = []
                                for fp in raw_file_path:
                                    # Make relative paths relative to data_directory if not absolute
                                    if not os.path.isabs(fp):
                                        fp = os.path.join(data_directory, fp)
                                    
                                    if not os.path.exists(fp):
                                        missing_files.append(fp)
                                    else:
                                        file_path.append(fp)
                                
                                if missing_files:
                                    logger.warning(f"{config.source_type.upper()} files not found: {missing_files}, skipping {table_name}")
                                    all_results[table_name] = {
                                        'error': f'{config.source_type.upper()} files not found: {missing_files}',
                                        'missing_files': missing_files
                                    }
                                    continue
                                    
                                if not file_path:  # All files were missing
                                    logger.warning(f"No valid {config.source_type.upper()} files found, skipping {table_name}")
                                    all_results[table_name] = {
                                        'error': f'No valid {config.source_type.upper()} files found'
                                    }
                                    continue
                            else:
                                # Single file
                                file_path = raw_file_path
                                # Make relative paths relative to data_directory if not absolute
                                if not os.path.isabs(file_path):
                                    file_path = os.path.join(data_directory, file_path)
                                
                                if not os.path.exists(file_path):
                                    logger.warning(f"{config.source_type.upper()} file not found: {file_path}, skipping {table_name}")
                                    all_results[table_name] = {
                                        'error': f'{config.source_type.upper()} file not found',
                                        'file_path': file_path
                                    }
                                    continue
                        else:
                            logger.error(f"source_file_path is required for {config.source_type} source type in table {table_name}")
                            all_results[table_name] = {
                                'error': f'source_file_path is required for {config.source_type} source type'
                            }
                            continue
                    
                    # Run ETL for this table
                    table_results = self.run_etl_for_table(config, file_path, transformation_options)
                    all_results[table_name] = table_results
                    
                except Exception as e:
                    logger.error(f"Failed to process table {table_name}: {e}")
                    all_results[table_name] = {
                        'error': str(e),
                        'table_name': table_name
                    }
                    continue
            
            logger.info("Completed processing all tables")
            return all_results
            
        except Exception as e:
            logger.error(f"Failed to run multiple tables ETL: {e}")
            raise
        finally:
            # Always disconnect
            if self.connection.is_connected():
                self.connection.disconnect()
    
    def run_etl(self, csv_file_path: str, table_name: str, 
                drop_if_exists: bool = True, batch_size: int = 1000, **csv_kwargs) -> None:
        """
        Run the complete ETL pipeline (backward compatibility method).
        
        Args:
            csv_file_path: Path to the CSV file
            table_name: Target Oracle table name
            drop_if_exists: Whether to drop existing table
            batch_size: Batch size for data insertion
            **csv_kwargs: Additional arguments for CSV reading
            
        Raises:
            Exception: If ETL pipeline fails
        """
        try:
            # Create a simple configuration
            config = TableConfig({
                'source_type': 'csv',
                'target_table': table_name,
                'batch_size': batch_size,
                'drop_if_exists': drop_if_exists,
                'csv_options': csv_kwargs
            })
            
            self.run_etl_with_config(config, csv_file_path)
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def get_pipeline_summary(self, results: Dict[str, Dict[str, Any]]) -> str:
        """
        Generate a summary of pipeline execution results.
        
        Args:
            results: Results dictionary from run_multiple_tables
            
        Returns:
            Formatted summary string
        """
        total_tables = len(results)
        successful_tables = sum(1 for r in results.values() if r.get('loading_success', False))
        failed_tables = total_tables - successful_tables
        total_rows = sum(r.get('total_rows', 0) for r in results.values())
        
        summary_lines = [
            "ETL Pipeline Execution Summary",
            "=" * 35,
            f"Total tables processed: {total_tables}",
            f"Successful: {successful_tables}",
            f"Failed: {failed_tables}",
            f"Total rows processed: {total_rows:,}",
            "",
            "Table Details:",
            "-" * 15
        ]
        
        for table_name, result in results.items():
            if result.get('error'):
                summary_lines.append(f"❌ {table_name}: {result['error']}")
            else:
                rows = result.get('total_rows', 0)
                summary_lines.append(f"✅ {table_name}: {rows:,} rows")
        
        return "\n".join(summary_lines)
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection test passes
        """
        return self.connection.test_connection()
    
    def __enter__(self) -> 'OracleETL':
        """Context manager entry."""
        self.connection.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.connection.disconnect()