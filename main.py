import logging
import os
from etl_dbf_oracle import TableConfig, OracleETL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Example usage of the Oracle ETL pipeline with configuration files."""
    
    try:
        # Load ETL instance from .env file
        etl = OracleETL.from_env_file('.env')
        etl.run_multiple_tables('config/table_config.yaml', 'data/')
        
    except Exception as e:
        print(f"ETL failed: {e}")

if __name__ == "__main__":
    main()