"""
Logger module for centralized logging configuration.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


class Logger:
    """
    Centralized logger for the application.
    """
    
    _loggers = {}
    
    @classmethod
    def get_logger(cls, name: str, log_file: str = None) -> logging.Logger:
        """
        Get or create a logger instance.
        
        Args:
            name: Name of the logger (usually __name__)
            log_file: Optional path to log file
            
        Returns:
            logging.Logger: Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        
        # Avoid adding handlers multiple times
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # File handler (if log_file provided)
            if log_file:
                log_path = Path(log_file)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                
                file_handler = logging.FileHandler(log_file)
                file_handler.setLevel(logging.DEBUG)
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                file_handler.setFormatter(file_formatter)
                logger.addHandler(file_handler)
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def log_error(cls, logger: logging.Logger, error: Exception, context: str = ""):
        """
        Log error with context information.
        
        Args:
            logger: Logger instance
            error: Exception object
            context: Additional context about where the error occurred
        """
        error_msg = f"{context}: {type(error).__name__} - {str(error)}" if context else str(error)
        logger.error(error_msg, exc_info=True)
    
    @classmethod
    def log_data_quality_issue(cls, logger: logging.Logger, issue_type: str, details: dict):
        """
        Log data quality issues in a structured format.
        
        Args:
            logger: Logger instance
            issue_type: Type of data quality issue
            details: Dictionary with issue details
        """
        logger.warning(f"DATA QUALITY ISSUE - {issue_type}: {details}")
