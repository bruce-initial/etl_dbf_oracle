import logging
import os
from etl_dbf_oracle import TableConfig, OracleETL
from etl_dbf_oracle.utils.logging_config import setup_pipeline_logging, log_etl_summary

# Setup file-based logging
log_files = setup_pipeline_logging(log_dir="logs")
logger = logging.getLogger(__name__)


def main():
    """Example usage of the Oracle ETL pipeline with configuration files."""
    
    logger.info("Starting ETL pipeline execution")
    logger.info(f"Log files: {log_files}")
    
    try:
        # Load ETL instance from .env file
        logger.info("Initializing Oracle ETL connection")
        etl = OracleETL.from_env_file('.env')
        
        # Run pipeline
        logger.info("Running ETL pipeline for multiple tables")
        results = etl.run_multiple_tables('config/table_config.yaml', 'data/')
        
        # Log comprehensive summary
        log_etl_summary(results, log_files)
        
        # Print basic results to console
        print(etl.get_pipeline_summary(results))
        
        logger.info("ETL pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}", exc_info=True)
        print(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    main()