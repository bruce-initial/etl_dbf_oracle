"""
Table configuration management for ETL pipeline.
"""

import os
import yaml
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class TableConfig:
    """Configuration class for table-specific ETL settings."""
    
    def __init__(self, config_dict: Dict[str, Any]):
        """
        Initialize table configuration from dictionary.
        
        Args:
            config_dict: Dictionary containing table configuration
        """
        self.source_type = config_dict.get('source_type', 'csv')
        self.source_table = config_dict.get('source_table')
        self.source_schema = config_dict.get('source_schema')
        self.source_file_path = config_dict.get('source_file_path')  # Explicit file path for CSV/DBF sources
        self.target_table = config_dict.get('target_table')
        self.target_type = config_dict.get('target_type', 'oracle')  # oracle, dbf
        self.target_file_path = config_dict.get('target_file_path')  # Explicit file path for DBF targets
        self.batch_size = config_dict.get('batch_size', 1000)
        self.custom_query_file = config_dict.get('custom_query')
        self.primary_key = config_dict.get('primary_key', [])
        self.foreign_keys = config_dict.get('foreign_keys', [])
        self.indexes = config_dict.get('indexes', [])
        self.drop_if_exists = config_dict.get('drop_if_exists', True)
        
        # Additional CSV-specific options
        self.csv_options = config_dict.get('csv_options', {})
        
        # Additional DBF-specific options
        self.dbf_options = config_dict.get('dbf_options', {})
        
        # Additional XLSX-specific options
        self.xlsx_options = config_dict.get('xlsx_options', {})
        
        # Data source configuration for custom queries
        self.data_source = config_dict.get('data_source', {})
        
        # Validate required fields
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        if not self.target_table:
            raise ValueError("target_table is required in configuration")
        
        # Validate source type
        valid_source_types = ['csv', 'dbf', 'xlsx', 'table', 'custom_query']
        if self.source_type.lower() not in valid_source_types:
            raise ValueError(f"source_type must be one of: {valid_source_types}")
        
        # Validate target type
        valid_target_types = ['oracle', 'dbf', 'xlsx']
        if self.target_type.lower() not in valid_target_types:
            raise ValueError(f"target_type must be one of: {valid_target_types}")
        
        # Validate file-based source types
        if self.source_type.lower() == 'csv':
            if not self.source_file_path:
                raise ValueError("source_file_path is required for CSV source type")
        elif self.source_type.lower() == 'dbf':
            if not self.source_file_path:
                raise ValueError("source_file_path is required for DBF source type")
        elif self.source_type.lower() == 'xlsx':
            if not self.source_file_path:
                raise ValueError("source_file_path is required for XLSX source type")
        elif self.source_type.lower() == 'table':
            if not self.source_table:
                raise ValueError("source_table is required for table source type")
        
        # Validate file-based target types
        if self.target_type.lower() == 'dbf':
            if not self.target_file_path:
                raise ValueError("target_file_path is required for DBF target type")
        elif self.target_type.lower() == 'xlsx':
            if not self.target_file_path:
                raise ValueError("target_file_path is required for XLSX target type")
        
        if self.source_type.lower() == 'custom_query':
            if not self.custom_query_file:
                raise ValueError("custom_query file path is required for custom_query source type")
            if not self.data_source:
                raise ValueError("data_source is required for custom_query source type")
            
            # Validate data_source configuration
            ds_type = self.data_source.get('type')
            if not ds_type:
                raise ValueError("data_source.type is required (csv, dbf, xlsx, or table)")
            
            if ds_type.lower() == 'csv' and not self.data_source.get('file_path'):
                raise ValueError("data_source.file_path is required for CSV data source")
            elif ds_type.lower() == 'dbf' and not self.data_source.get('file_path'):
                raise ValueError("data_source.file_path is required for DBF data source")
            elif ds_type.lower() == 'xlsx' and not self.data_source.get('file_path'):
                raise ValueError("data_source.file_path is required for XLSX data source")
            elif ds_type.lower() == 'table' and not self.data_source.get('table_name'):
                raise ValueError("data_source.table_name is required for table data source")
    
    @classmethod
    def load_from_yaml(cls, config_file: str) -> Dict[str, 'TableConfig']:
        """
        Load table configurations from YAML file.
        
        Args:
            config_file: Path to YAML configuration file
            
        Returns:
            Dictionary of table name to TableConfig objects
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            yaml.YAMLError: If YAML parsing fails
        """
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}")
            raise
        
        if not config_data or 'tables' not in config_data:
            raise ValueError("Configuration file must contain 'tables' section")
        
        tables_config = {}
        for table_name, table_config in config_data.get('tables', {}).items():
            try:
                tables_config[table_name] = cls(table_config)
                logger.debug(f"Loaded configuration for table: {table_name}")
            except Exception as e:
                logger.error(f"Failed to load configuration for table {table_name}: {e}")
                raise
        
        logger.info(f"Successfully loaded {len(tables_config)} table configurations")
        return tables_config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary format."""
        return {
            'source_type': self.source_type,
            'source_table': self.source_table,
            'source_schema': self.source_schema,
            'source_file_path': self.source_file_path,
            'target_table': self.target_table,
            'target_type': self.target_type,
            'target_file_path': self.target_file_path,
            'batch_size': self.batch_size,
            'custom_query': self.custom_query_file,
            'primary_key': self.primary_key,
            'foreign_keys': self.foreign_keys,
            'indexes': self.indexes,
            'drop_if_exists': self.drop_if_exists,
            'csv_options': self.csv_options,
            'dbf_options': self.dbf_options,
            'xlsx_options': self.xlsx_options,
            'data_source': self.data_source
        }
    
    def __repr__(self) -> str:
        """String representation of configuration."""
        return f"TableConfig(target_table='{self.target_table}', source_type='{self.source_type}')"