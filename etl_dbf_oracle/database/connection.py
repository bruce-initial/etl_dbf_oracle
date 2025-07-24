"""
Oracle database connection management.
"""

import os
import oracledb
import logging
from typing import Dict, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class OracleConnection:
    """Manages Oracle database connections with environment variable support."""
    
    def __init__(self, connection_params: Optional[Dict[str, str]] = None):
        """
        Initialize Oracle connection manager.
        
        Args:
            connection_params: Dictionary with Oracle connection parameters
                              (user, password, dsn, host, port, service_name)
        """
        self.connection_params = connection_params or {}
        self.connection: Optional[oracledb.Connection] = None
        self._is_connected = False
    
    @classmethod
    def from_env_file(cls, env_file: str = '.env') -> 'OracleConnection':
        """
        Create OracleConnection instance from environment file.
        
        Args:
            env_file: Path to .env file (default: '.env')
            
        Returns:
            OracleConnection instance with connection parameters from .env
            
        Raises:
            ValueError: If required environment variables are missing
        """
        load_dotenv(env_file)
        
        # Get connection parameters from environment variables
        connection_params = {
            'user': os.getenv('ORACLE_USER'),
            'password': os.getenv('ORACLE_PASSWORD'),
            'host': os.getenv('ORACLE_HOST', 'localhost'),
            'port': int(os.getenv('ORACLE_PORT', '1521')),
            'service_name': os.getenv('ORACLE_SERVICE_NAME', 'XEPDB1')
        }
        
        # Alternative: use DSN if provided
        dsn = os.getenv('ORACLE_DSN')
        if dsn:
            connection_params = {
                'user': os.getenv('ORACLE_USER'),
                'password': os.getenv('ORACLE_PASSWORD'),
                'dsn': dsn
            }
        
        # Validate required parameters
        if not connection_params['user'] or not connection_params['password']:
            raise ValueError(
                "ORACLE_USER and ORACLE_PASSWORD must be set in environment variables"
            )
        
        logger.info(f"Loaded Oracle connection parameters from {env_file}")
        return cls(connection_params)
    
    def connect(self) -> None:
        """
        Establish connection to Oracle database.
        
        Raises:
            oracledb.Error: If connection fails
        """
        if self._is_connected and self.connection:
            logger.debug("Already connected to Oracle database")
            return
        
        try:
            # Create connection string
            if 'dsn' in self.connection_params:
                dsn = self.connection_params['dsn']
            else:
                host = self.connection_params.get('host', 'localhost')
                port = self.connection_params.get('port', 1521)
                service_name = self.connection_params.get('service_name', 'XEPDB1')
                dsn = f"{host}:{port}/{service_name}"
            
            self.connection = oracledb.connect(
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                dsn=dsn
            )
            self._is_connected = True
            logger.info("Successfully connected to Oracle database")
            
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            self._is_connected = False
            raise
    
    def disconnect(self) -> None:
        """Close Oracle database connection."""
        if self.connection and self._is_connected:
            try:
                self.connection.close()
                logger.info("Disconnected from Oracle database")
            except Exception as e:
                logger.warning(f"Error during disconnect: {e}")
            finally:
                self.connection = None
                self._is_connected = False
    
    def get_connection(self) -> oracledb.Connection:
        """
        Get active Oracle connection.
        
        Returns:
            Active Oracle connection
            
        Raises:
            RuntimeError: If not connected to database
        """
        if not self._is_connected or not self.connection:
            raise RuntimeError("Not connected to Oracle database. Call connect() first.")
        return self.connection
    
    def is_connected(self) -> bool:
        """Check if connected to Oracle database."""
        return self._is_connected and self.connection is not None
    
    def test_connection(self) -> bool:
        """
        Test Oracle database connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if not self._is_connected:
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            
            return result is not None
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def __enter__(self) -> 'OracleConnection':
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
    
    def __del__(self) -> None:
        """Destructor to ensure connection is closed."""
        if hasattr(self, '_is_connected') and self._is_connected:
            self.disconnect()