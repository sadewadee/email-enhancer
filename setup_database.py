#!/usr/bin/env python3
"""
Setup PostgreSQL database for Email Enhancer
Creates the scraped_contacts table with all required columns.

Usage:
    python setup_database.py

Requires:
    - psycopg2-binary
    - python-dotenv
    - .env file with database credentials
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
import sys
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Create scraped_contacts table in PostgreSQL database."""

    # Load .env file
    load_dotenv()

    # Get database credentials
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME', 'zenvoyer_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }

    logger.info(f"Connecting to PostgreSQL: {db_config['host']}:{db_config['port']}/{db_config['database']}")

    try:
        # Connect to database
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        logger.info("✓ Connection successful")

        # Read SQL file
        sql_file = os.path.join(os.path.dirname(__file__), 'create_table.sql')

        if not os.path.exists(sql_file):
            logger.error(f"SQL file not found: {sql_file}")
            sys.exit(1)

        with open(sql_file, 'r') as f:
            sql = f.read()

        logger.info("Executing CREATE TABLE script...")

        # Execute entire SQL script at once (psycopg2 handles multiple statements)
        try:
            cursor.execute(sql)
            logger.info("✓ SQL script executed successfully")
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            raise

        logger.info("✓ CREATE TABLE script executed successfully")

        # Verify table exists
        cursor.execute("""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns
            WHERE table_name = 'scraped_contacts';
        """)
        column_count = cursor.fetchone()[0]

        if column_count > 0:
            logger.info(f"✓ Table 'scraped_contacts' created with {column_count} columns")

            # Show table structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'scraped_contacts'
                ORDER BY ordinal_position
                LIMIT 10;
            """)

            logger.info("\nFirst 10 columns:")
            for row in cursor.fetchall():
                col_name, data_type, nullable = row
                logger.info(f"  - {col_name}: {data_type} (nullable: {nullable})")

            logger.info(f"  ... and {column_count - 10} more columns")
        else:
            logger.error("✗ Table creation failed - no columns found")
            sys.exit(1)

        # Close connection
        cursor.close()
        conn.close()

        logger.info("\n" + "="*60)
        logger.info("SUCCESS! Database setup complete.")
        logger.info("="*60)
        logger.info("\nNext steps:")
        logger.info("1. Install dependencies: pip install psycopg2-binary python-dotenv")
        logger.info("2. Test scraping with DB export: python main.py single test.csv --workers 2 --export-db")
        logger.info("3. Verify data: SELECT COUNT(*) FROM scraped_contacts;")

    except psycopg2.OperationalError as e:
        logger.error(f"✗ Connection failed: {e}")
        logger.error(f"\nCheck your .env file:")
        logger.error(f"  DB_HOST={db_config['host']}")
        logger.error(f"  DB_PORT={db_config['port']}")
        logger.error(f"  DB_NAME={db_config['database']}")
        logger.error(f"  DB_USER={db_config['user']}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"✗ Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
