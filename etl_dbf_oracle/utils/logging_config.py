"""
Logging configuration for ETL pipeline.
"""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path


def setup_file_logging(log_level: str = "INFO", log_dir: str = "logs"):
    """
    Configure file-based logging for ETL pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory to store log files
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Generate timestamp for log files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Console handler (for immediate feedback)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)
    
    # Process log file handler (all logs)
    process_log_file = log_path / f"etl_process_{timestamp}.log"
    process_handler = logging.FileHandler(process_log_file, encoding='utf-8')
    process_handler.setLevel(logging.DEBUG)
    process_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(process_handler)
    
    # Error log file handler (errors and warnings only)
    error_log_file = log_path / f"etl_errors_{timestamp}.log"
    error_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_handler.setLevel(logging.WARNING)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)
    
    # Rotating file handler for long-running processes
    rotating_log_file = log_path / "etl_rotating.log"
    rotating_handler = logging.handlers.RotatingFileHandler(
        rotating_log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    rotating_handler.setLevel(logging.INFO)
    rotating_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(rotating_handler)
    
    # Log the setup
    logging.info(f"File logging configured:")
    logging.info(f"  Process log: {process_log_file}")
    logging.info(f"  Error log: {error_log_file}")
    logging.info(f"  Rotating log: {rotating_log_file}")
    logging.info(f"  Log level: {log_level.upper()}")
    
    return {
        'process_log': str(process_log_file),
        'error_log': str(error_log_file),
        'rotating_log': str(rotating_log_file)
    }


def setup_pipeline_logging(table_name: str = None, log_dir: str = "logs"):
    """
    Configure logging specifically for a pipeline run.
    
    Args:
        table_name: Optional table name for specialized logging
        log_dir: Directory to store log files
        
    Returns:
        Dictionary with log file paths
    """
    # Get log level from environment or default to INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    # Setup base logging
    log_files = setup_file_logging(log_level, log_dir)
    
    # Add table-specific logging if provided
    if table_name:
        log_path = Path(log_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        table_log_file = log_path / f"table_{table_name}_{timestamp}.log"
        table_handler = logging.FileHandler(table_log_file, encoding='utf-8')
        table_handler.setLevel(logging.DEBUG)
        
        # Create a filter for table-specific logging
        class TableFilter:
            def __init__(self, table_name):
                self.table_name = table_name
            
            def filter(self, record):
                return self.table_name.lower() in record.getMessage().lower()
        
        table_handler.addFilter(TableFilter(table_name))
        table_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        table_handler.setFormatter(table_formatter)
        
        logging.getLogger().addHandler(table_handler)
        log_files['table_log'] = str(table_log_file)
        
        logging.info(f"Table-specific logging enabled: {table_log_file}")
    
    return log_files


def log_etl_summary(results: dict, log_files: dict = None):
    """
    Log a summary of ETL pipeline results.
    
    Args:
        results: ETL results dictionary
        log_files: Dictionary of log file paths
    """
    logger = logging.getLogger(__name__)
    
    total_tables = len(results)
    successful_tables = sum(1 for r in results.values() if r.get('loading_success', False))
    failed_tables = total_tables - successful_tables
    total_rows = sum(r.get('total_rows', 0) for r in results.values())
    
    summary_lines = [
        "=" * 60,
        "ETL PIPELINE EXECUTION SUMMARY",
        "=" * 60,
        f"Total tables processed: {total_tables}",
        f"Successful: {successful_tables}",
        f"Failed: {failed_tables}",
        f"Total rows processed: {total_rows:,}",
        "",
        "TABLE RESULTS:",
        "-" * 40
    ]
    
    for table_name, result in results.items():
        if result.get('error'):
            summary_lines.append(f"❌ {table_name}: {result['error']}")
        else:
            rows = result.get('total_rows', 0)
            summary_lines.append(f"✅ {table_name}: {rows:,} rows")
    
    summary_lines.extend([
        "-" * 40,
        f"Log files generated:",
    ])
    
    if log_files:
        for log_type, log_path in log_files.items():
            summary_lines.append(f"  {log_type}: {log_path}")
    
    summary_lines.append("=" * 60)
    
    # Log the summary
    for line in summary_lines:
        logger.info(line)