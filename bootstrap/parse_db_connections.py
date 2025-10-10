
#!/usr/bin/env python3
"""
Enhanced Database Connection Parser for ClickHouse Multi-Database Setup
======================================================================

Parses DB_CONNECTIONS environment variable and creates:
- Multiple ClickHouse databases (one per MySQL connection)
- Granular user permissions for each database
- Comprehensive logging of all operations
"""

import os
import json
import logging
from datetime import datetime
from clickhouse_driver import Client

# Configure comprehensive logging
def setup_logging():
    """Setup detailed logging with multiple handlers"""
    os.makedirs('/app/logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
    )
    
    logger = logging.getLogger('DBConnectionParser')
    
    # File handler for detailed logs
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(f'/app/logs/db_parser_{timestamp}.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

def get_clickhouse_client():
    """Get ClickHouse client with enhanced error handling"""
    try:
        client = Client(
            host=os.getenv('CLICKHOUSE_HTTP_HOST', 'clickhouse'),
            port=int(os.getenv('CLICKHOUSE_NATIVE_PORT', 9000)),
            user='default',
            password='',
            database='default',
            connect_timeout=10,
            send_receive_timeout=30
        )
        
        # Test connection
        client.execute("SELECT 1")
        logger.info("✅ ClickHouse client connected and tested successfully")
        return client
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to ClickHouse: {e}")
        return None

def parse_db_connections():
    """Parse and validate DB_CONNECTIONS with detailed logging"""
    db_connections_str = os.getenv('DB_CONNECTIONS', '[]')
    logger.info(f"📋 DB_CONNECTIONS raw: {db_connections_str}")
    
    try:
        # Limpiar comillas externas si existen
        if db_connections_str.startswith("'") and db_connections_str.endswith("'"):
            db_connections_str = db_connections_str[1:-1]
            
        connections = json.loads(db_connections_str)
        logger.info(f"✅ Parsed {len(connections)} database connections")
        
        # Log each connection details
        for i, conn in enumerate(connections):
            name = conn.get('name', f'unnamed_{i}')
            host = conn.get('host', 'unknown')
            port = conn.get('port', 'unknown')
            db = conn.get('db', 'unknown')
            user = conn.get('user', 'unknown')
            
            logger.info(f"   Connection {i+1}: {name}")
            logger.info(f"     MySQL: {db}@{host}:{port} (user: {user})")
            logger.info(f"     ClickHouse: fgeo_{name}")
        
        return connections
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to parse DB_CONNECTIONS: {e}")
        return []

def create_clickhouse_databases(client, connections):
    """Create ClickHouse databases with comprehensive setup"""
    created_databases = []
    
    for i, conn in enumerate(connections):
        db_name = conn.get('name', f'default_{i}')
        mysql_db = conn.get('db', 'unknown')
        mysql_host = conn.get('host', 'unknown')
        mysql_port = conn.get('port', 3306)
        mysql_user = conn.get('user', 'unknown')
        
        clickhouse_db = f"fgeo_{db_name}"
        
        try:
            logger.info(f"🏗️  Creating database: {clickhouse_db}")
            logger.info(f"     Source: {mysql_db}@{mysql_host}:{mysql_port}")
            
            # Create database
            client.execute(f"CREATE DATABASE IF NOT EXISTS {clickhouse_db}")
            
            # Create connection metadata table
            client.execute(f"""
            CREATE TABLE IF NOT EXISTS {clickhouse_db}.connection_metadata (
                connection_name String,
                mysql_database String,
                mysql_host String,
                mysql_port UInt16,
                mysql_user String,
                created_at DateTime DEFAULT now(),
                last_updated DateTime DEFAULT now(),
                status String DEFAULT 'active'
            )
            ENGINE = MergeTree()
            ORDER BY (connection_name, created_at)
            """)
            
            # Create audit table for permissions
            client.execute(f"""
            CREATE TABLE IF NOT EXISTS {clickhouse_db}.permission_audit (
                username String,
                permission_type String,
                table_name String DEFAULT '*',
                granted_at DateTime DEFAULT now(),
                granted_by String DEFAULT 'system'
            )
            ENGINE = MergeTree()
            ORDER BY (username, granted_at)
            """)
            
            # Insert connection metadata
            client.execute(f"""
            INSERT INTO {clickhouse_db}.connection_metadata
            (connection_name, mysql_database, mysql_host, mysql_port, mysql_user)
            VALUES (%(name)s, %(db)s, %(host)s, %(port)s, %(user)s)
            """, {
                'name': db_name,
                'db': mysql_db,
                'host': mysql_host,
                'port': mysql_port,
                'user': mysql_user
            })
            
            created_databases.append(clickhouse_db)
            logger.info(f"✅ Database {clickhouse_db} created successfully")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed to create database {clickhouse_db}: {error_msg}")
    
    logger.info(f"📊 Created {len(created_databases)} databases: {', '.join(created_databases)}")
    return created_databases

def setup_users_and_permissions(client, connections):
    """Setup users with granular permissions for each database"""
    users_to_create = [
        ('etl', 'Et1Ingest!', 'ETL processing user with full permissions'),
        ('superset', 'Sup3rS3cret!', 'Superset user with read permissions'),
        ('auditor', 'Audit0r123!', 'Auditor user for validation and monitoring')
    ]
    
    try:
        logger.info("👤 Setting up users with granular permissions...")
        
        # Create users
        for username, password, description in users_to_create:
            try:
                client.execute(f"CREATE USER IF NOT EXISTS {username} IDENTIFIED BY '{password}'")
                logger.info(f"✅ User created: {username} ({description})")
            except Exception as e:
                logger.warning(f"⚠️  User {username} may already exist: {e}")
        
        # Grant system-level permissions
        try:
            # ETL user - system permissions
            client.execute("GRANT SHOW DATABASES ON *.* TO etl")
            client.execute("GRANT SHOW TABLES ON *.* TO etl")
            client.execute("GRANT CREATE DATABASE ON *.* TO etl")
            
            # Auditor - monitoring permissions
            client.execute("GRANT SHOW DATABASES ON *.* TO auditor")
            client.execute("GRANT SHOW TABLES ON *.* TO auditor")
            client.execute("GRANT SELECT ON system.* TO auditor")
            
            # Superset - basic permissions
            client.execute("GRANT SHOW DATABASES ON *.* TO superset")
            client.execute("GRANT SHOW TABLES ON *.* TO superset")
            
            logger.info("✅ System-level permissions granted")
            
        except Exception as e:
            logger.warning(f"⚠️  Some system permissions may already exist: {e}")
        
        # Grant database-specific permissions
        for conn in connections:
            db_name = conn.get('name', 'default')
            clickhouse_db = f"fgeo_{db_name}"
            
            try:
                logger.info(f"🔐 Setting permissions for {clickhouse_db}")
                
                # ETL user - full permissions on this database
                client.execute(f"GRANT ALL ON {clickhouse_db}.* TO etl WITH GRANT OPTION")
                client.execute(f"INSERT INTO {clickhouse_db}.permission_audit (username, permission_type) VALUES ('etl', 'ALL')")
                
                # Superset user - read permissions on this database
                client.execute(f"GRANT SELECT ON {clickhouse_db}.* TO superset")
                client.execute(f"INSERT INTO {clickhouse_db}.permission_audit (username, permission_type) VALUES ('superset', 'SELECT')")
                
                # Auditor user - read and monitoring permissions
                client.execute(f"GRANT SELECT ON {clickhouse_db}.* TO auditor")
                client.execute(f"INSERT INTO {clickhouse_db}.permission_audit (username, permission_type) VALUES ('auditor', 'SELECT')")
                
                logger.info(f"✅ Permissions configured for {clickhouse_db}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Failed to set permissions for {clickhouse_db}: {error_msg}")
        
        logger.info("✅ All users and permissions configured successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to setup users and permissions: {e}")
        raise

def main():
    """Main function with comprehensive error handling"""
    logger.info("🚀 Starting Enhanced ClickHouse Multi-Database setup")
    logger.info("=" * 70)
    
    try:
        # Get ClickHouse client
        client = get_clickhouse_client()
        if not client:
            logger.error("❌ Cannot proceed without ClickHouse connection")
            return 1
        
        # Parse database connections
        connections = parse_db_connections()
        if not connections:
            logger.warning("⚠️  No database connections found, using default configuration")
            connections = [{"name": "default", "db": "archivos", "host": "unknown", "port": 3306, "user": "unknown"}]
        
        # Create individual databases
        created_databases = create_clickhouse_databases(client, connections)
        
        # Setup users and permissions
        setup_users_and_permissions(client, connections)
        
        logger.info("🎉 ClickHouse multi-database setup completed successfully!")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Setup failed with critical error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
