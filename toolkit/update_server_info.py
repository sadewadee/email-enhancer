#!/usr/bin/env python3
"""
Update Server Info Script

Auto-detect and update server IP and region in zen_servers table.
Run this on each server after deployment to populate server metadata.

Usage:
    python toolkit/update_server_info.py                    # Auto-detect for current server
    python toolkit/update_server_info.py --server-id id-01  # Specific server
    python toolkit/update_server_info.py --region ID        # Override region
    python toolkit/update_server_info.py --list             # List all servers
"""

import argparse
import logging
import os
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database_writer import create_database_writer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def detect_server_ip() -> str:
    """Auto-detect server IP address."""
    hostname = socket.gethostname()
    
    # Try hostname resolution
    try:
        ip = socket.gethostbyname(hostname)
        if ip and not ip.startswith('127.'):
            return ip
    except (socket.gaierror, socket.herror):
        pass
    
    # Fallback: UDP trick to get local IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        pass
    
    return None


def detect_region(server_id: str) -> str:
    """Auto-detect region from server_id prefix (e.g., id-01 -> ID)."""
    # Try env var first
    env_region = os.environ.get('SERVER_REGION')
    if env_region:
        return env_region.upper()[:2]
    
    # Parse from server_id prefix
    if '-' in server_id:
        prefix = server_id.split('-')[0].upper()
        if len(prefix) == 2 and prefix.isalpha():
            return prefix
    
    return None


def list_servers(db):
    """List all registered servers."""
    conn = db.pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT server_id, server_name, server_ip, server_region, 
                   server_hostname, status, last_heartbeat
            FROM zen_servers
            ORDER BY server_id
        """)
        rows = cursor.fetchall()
        
        if not rows:
            print("No servers registered.")
            return
        
        print(f"\n{'Server ID':<15} {'Name':<20} {'IP':<16} {'Region':<8} {'Status':<10} {'Last Heartbeat'}")
        print("-" * 90)
        for row in rows:
            server_id, name, ip, region, hostname, status, heartbeat = row
            hb_str = heartbeat.strftime('%Y-%m-%d %H:%M') if heartbeat else '-'
            print(f"{server_id or '-':<15} {(name or '-'):<20} {(ip or '-'):<16} {(region or '-'):<8} {(status or '-'):<10} {hb_str}")
        print()
    finally:
        db.pool.putconn(conn)


def update_server_info(db, server_id: str, ip: str = None, region: str = None):
    """Update server IP and region in zen_servers table."""
    conn = db.pool.getconn()
    try:
        cursor = conn.cursor()
        
        # Build dynamic UPDATE
        updates = []
        params = []
        
        if ip:
            updates.append("server_ip = %s")
            params.append(ip)
        
        if region:
            updates.append("server_region = %s")
            params.append(region)
        
        if not updates:
            logger.warning("Nothing to update")
            return False
        
        params.append(server_id)
        
        query = f"""
            UPDATE zen_servers 
            SET {', '.join(updates)}
            WHERE server_id = %s
        """
        
        cursor.execute(query, params)
        rows_affected = cursor.rowcount
        conn.commit()
        
        if rows_affected > 0:
            logger.info(f"Updated server {server_id}: IP={ip}, Region={region}")
            return True
        else:
            logger.warning(f"Server {server_id} not found in zen_servers")
            return False
            
    except Exception as e:
        logger.error(f"Failed to update server info: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        db.pool.putconn(conn)


def main():
    parser = argparse.ArgumentParser(description='Update server IP and region in database')
    parser.add_argument('--server-id', '-s', help='Server ID to update (default: auto-detect from hostname)')
    parser.add_argument('--ip', help='Override IP address (default: auto-detect)')
    parser.add_argument('--region', '-r', help='Override region code (default: auto-detect from server-id)')
    parser.add_argument('--list', '-l', action='store_true', help='List all registered servers')
    args = parser.parse_args()
    
    # Create database connection
    db = create_database_writer(logger)
    if not db or not db.connect():
        logger.error("Failed to connect to database")
        sys.exit(1)
    
    try:
        if args.list:
            list_servers(db)
            return
        
        # Determine server_id
        server_id = args.server_id or socket.gethostname()
        
        # Auto-detect IP
        ip = args.ip or detect_server_ip()
        
        # Auto-detect region
        region = args.region or detect_region(server_id)
        
        logger.info(f"Server ID: {server_id}")
        logger.info(f"Detected IP: {ip}")
        logger.info(f"Detected Region: {region}")
        
        if not ip and not region:
            logger.error("Could not detect IP or region. Use --ip and --region flags.")
            sys.exit(1)
        
        # Update database
        success = update_server_info(db, server_id, ip, region)
        sys.exit(0 if success else 1)
        
    finally:
        db.close()


if __name__ == '__main__':
    main()
