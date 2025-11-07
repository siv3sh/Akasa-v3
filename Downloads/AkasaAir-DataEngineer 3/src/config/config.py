"""
Configuration module for managing environment variables and database connections.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)


class Config:
    """
    Configuration class to manage application settings.
    """
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '3306')
    DB_USER = os.getenv('DB_USER', 'root')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_NAME = os.getenv('DB_NAME', 'akasa_air_db')
    # Optional: UNIX socket path for local MySQL (helps avoid host-based auth issues)
    DB_SOCKET = os.getenv('DB_SOCKET', '')
    
    # Data Paths
    BASE_DIR = Path(__file__).parent.parent.parent
    CUSTOMERS_CSV_PATH = os.getenv('CUSTOMERS_CSV_PATH', str(BASE_DIR / 'data' / 'customers.csv'))
    ORDERS_XML_PATH = os.getenv('ORDERS_XML_PATH', str(BASE_DIR / 'data' / 'orders.xml'))
    
    # Output Directory
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', str(BASE_DIR / 'outputs'))
    
    @classmethod
    def validate_config(cls):
        """
        Validate that all required configuration values are present.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        required_fields = ['DB_HOST', 'DB_USER', 'DB_NAME']
        for field in required_fields:
            if not getattr(cls, field):
                return False
        return True
    
    @classmethod
    def get_database_url(cls):
        """
        Get the database connection URL.
        
        Returns:
            str: Database connection URL
        """
        base = f"mysql+pymysql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
        # If a UNIX socket is provided, use it to ensure local socket auth (common for Homebrew MySQL)
        if cls.DB_SOCKET:
            return f"{base}?unix_socket={cls.DB_SOCKET}"
        return base
