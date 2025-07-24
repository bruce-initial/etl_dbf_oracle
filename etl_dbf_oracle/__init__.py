"""
Oracle ETL Pipeline Package

A comprehensive, configuration-driven ETL pipeline for Oracle databases.
"""

__version__ = "0.1.0"

from .config.table_config import TableConfig
from .etl.pipeline import OracleETL

__all__ = ["TableConfig", "OracleETL"]