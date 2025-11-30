"""Dashboard configuration loader."""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv


@dataclass
class Config:
    """Dashboard configuration."""
    # Database
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    
    # Server
    host: str
    port: int
    debug: bool
    
    # Export
    export_max_rows: int
    export_timeout: int
    
    # Auth (optional)
    username: Optional[str]
    password: Optional[str]


def load_config() -> Config:
    """Load configuration from environment."""
    load_dotenv()
    
    return Config(
        db_host=os.getenv('DB_HOST', 'localhost'),
        db_port=int(os.getenv('DB_PORT', '5432')),
        db_name=os.getenv('DB_NAME', 'zenvoyer_db'),
        db_user=os.getenv('DB_USER', 'postgres'),
        db_password=os.getenv('DB_PASSWORD', ''),
        host=os.getenv('DASHBOARD_HOST', '0.0.0.0'),
        port=int(os.getenv('DASHBOARD_PORT', '8080')),
        debug=os.getenv('DASHBOARD_DEBUG', 'false').lower() == 'true',
        export_max_rows=int(os.getenv('EXPORT_MAX_ROWS', '100000')),
        export_timeout=int(os.getenv('EXPORT_TIMEOUT', '300')),
        username=os.getenv('DASHBOARD_USERNAME'),
        password=os.getenv('DASHBOARD_PASSWORD'),
    )
