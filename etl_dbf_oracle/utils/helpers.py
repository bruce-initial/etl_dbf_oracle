"""
Utility functions and helpers for ETL pipeline.
"""

import re
import logging
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


class ColumnSanitizer:
    """Handles column name sanitization for Oracle compatibility."""
    
    # Oracle reserved keywords (common ones)
    ORACLE_RESERVED_KEYWORDS: Set[str] = {
        'ACCESS', 'ADD', 'ALL', 'ALTER', 'AND', 'ANY', 'AS', 'ASC', 'AUDIT', 'BETWEEN', 
        'BY', 'CHAR', 'CHECK', 'CLUSTER', 'COLUMN', 'COMMENT', 'COMPRESS', 'CONNECT', 
        'CREATE', 'CURRENT', 'DATE', 'DECIMAL', 'DEFAULT', 'DELETE', 'DESC', 'DISTINCT', 
        'DROP', 'ELSE', 'EXCLUSIVE', 'EXISTS', 'FILE', 'FLOAT', 'FOR', 'FROM', 'GRANT', 
        'GROUP', 'HAVING', 'IDENTIFIED', 'IMMEDIATE', 'IN', 'INCREMENT', 'INDEX', 'INITIAL', 
        'INSERT', 'INTEGER', 'INTERSECT', 'INTO', 'IS', 'LEVEL', 'LIKE', 'LOCK', 'LONG', 
        'MAXEXTENTS', 'MINUS', 'MODE', 'MODIFY', 'NOAUDIT', 'NOCOMPRESS', 'NOT', 'NOWAIT', 
        'NULL', 'NUMBER', 'OF', 'OFFLINE', 'ON', 'ONLINE', 'OPTION', 'OR', 'ORDER', 'PCTFREE', 
        'PRIOR', 'PUBLIC', 'RAW', 'RENAME', 'RESOURCE', 'REVOKE', 'ROW', 'ROWID', 'ROWNUM', 
        'ROWS', 'SELECT', 'SESSION', 'SET', 'SHARE', 'SIZE', 'SMALLINT', 'START', 'SUCCESSFUL', 
        'SYNONYM', 'SYSDATE', 'TABLE', 'THEN', 'TO', 'TRIGGER', 'UID', 'UNION', 'UNIQUE', 
        'UPDATE', 'USER', 'VALIDATE', 'VALUES', 'VARCHAR', 'VARCHAR2', 'VIEW', 'WHENEVER', 
        'WHERE', 'WITH', 'TIMESTAMP', 'INTERVAL', 'PARTITION', 'SUBPARTITION'
    }
    
    @classmethod
    def sanitize_column_names(cls, columns: List[str]) -> Dict[str, str]:
        """
        Sanitize column names to avoid Oracle reserved keywords and invalid characters.
        
        Args:
            columns: List of original column names
            
        Returns:
            Dictionary mapping original names to sanitized names
        """
        column_mapping = {}
        used_names = set()
        
        for col in columns:
            # Remove special characters and spaces, replace with underscore
            sanitized = re.sub(r'[^\w]', '_', col.strip())
            
            # Remove leading/trailing underscores and multiple consecutive underscores
            sanitized = re.sub(r'^_+|_+$', '', sanitized)
            sanitized = re.sub(r'_+', '_', sanitized)
            
            # Ensure it doesn't start with a number
            if sanitized and sanitized[0].isdigit():
                sanitized = f"COL_{sanitized}"
            
            # Handle empty names
            if not sanitized:
                sanitized = "UNNAMED_COLUMN"
            
            # Convert to uppercase for Oracle comparison
            sanitized_upper = sanitized.upper()
            
            # Check if it's a reserved keyword
            if sanitized_upper in cls.ORACLE_RESERVED_KEYWORDS:
                sanitized = f"{sanitized}_COL"
                sanitized_upper = sanitized.upper()
            
            # Handle duplicates
            original_sanitized = sanitized
            counter = 1
            while sanitized_upper in used_names:
                sanitized = f"{original_sanitized}_{counter}"
                sanitized_upper = sanitized.upper()
                counter += 1
            
            # Limit length to Oracle's 30-character limit for older versions
            # (128 for Oracle 12.2+, but keeping conservative)
            if len(sanitized) > 30:
                sanitized = sanitized[:27] + f"_{counter:02d}"
                counter += 1
            
            used_names.add(sanitized_upper)
            column_mapping[col] = sanitized
            
        logger.debug(f"Sanitized {len(columns)} column names")
        return column_mapping


class ValidationHelpers:
    """Validation utilities for ETL pipeline."""
    
    @staticmethod
    def validate_file_path(file_path: str) -> bool:
        """
        Validate if file path exists and is readable.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file is valid and readable
        """
        import os
        
        if not file_path:
            return False
        
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            return False
        
        if not os.path.isfile(file_path):
            logger.error(f"Path is not a file: {file_path}")
            return False
        
        if not os.access(file_path, os.R_OK):
            logger.error(f"File is not readable: {file_path}")
            return False
        
        return True
    
    @staticmethod
    def validate_table_name(table_name: str) -> bool:
        """
        Validate Oracle table name format.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            True if table name is valid for Oracle
        """
        if not table_name:
            return False
        
        # Oracle table names must start with a letter
        if not table_name[0].isalpha():
            return False
        
        # Can only contain letters, numbers, and underscores
        if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', table_name):
            return False
        
        # Check length (30 chars for older Oracle versions)
        if len(table_name) > 30:
            return False
        
        # Check if it's a reserved keyword
        if table_name.upper() in ColumnSanitizer.ORACLE_RESERVED_KEYWORDS:
            return False
        
        return True
    
    @staticmethod
    def validate_batch_size(batch_size: int) -> bool:
        """
        Validate batch size parameter.
        
        Args:
            batch_size: Batch size to validate
            
        Returns:
            True if batch size is valid
        """
        return isinstance(batch_size, int) and 1 <= batch_size <= 100000