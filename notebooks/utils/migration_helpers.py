"""
migration_helpers.py

Helper functions for PySpark notebooks to facilitate migration from Azure Synapse to Fabric Warehouse.

This module provides:
- Connection utilities for Azure SQL Database and Fabric Warehouse
- ADLS storage connection helpers
- Logging and error handling utilities
- Common migration operations

Usage in PySpark notebooks:
    from migration_helpers import ConnectionHelper, MigrationUtils
    
    # Connect to Azure SQL Database
    conn = ConnectionHelper.connect_azure_sql(server, database, auth_config)
    
    # Connect to Fabric Warehouse
    conn = ConnectionHelper.connect_fabric_warehouse(workspace, warehouse, auth_config)
"""

import pyodbc
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import time


class Colors:
    """ANSI color codes for console output"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'


class ConnectionHelper:
    """Helper class for database connections"""
    
    @staticmethod
    def connect_azure_sql(server: str, database: str, auth_config: Optional[Dict] = None) -> pyodbc.Connection:
        """
        Connect to Azure SQL Database or Synapse Dedicated SQL Pool
        
        Args:
            server: Server name (e.g., myserver.database.windows.net)
            database: Database name
            auth_config: Optional authentication configuration with keys:
                - 'token': Access token for token-based auth
                - 'username': Username for SQL auth
                - 'password': Password for SQL auth
                - 'auth_type': 'token', 'sql', or 'interactive' (default: 'interactive')
        
        Returns:
            pyodbc.Connection: Database connection object
        
        Example:
            # Token-based authentication
            conn = ConnectionHelper.connect_azure_sql(
                "myserver.database.windows.net",
                "mydatabase",
                {"token": access_token, "auth_type": "token"}
            )
            
            # Interactive authentication (default)
            conn = ConnectionHelper.connect_azure_sql(
                "myserver.database.windows.net",
                "mydatabase"
            )
        """
        auth_type = auth_config.get('auth_type', 'interactive') if auth_config else 'interactive'
        
        print(f"{Colors.BLUE}Connecting to Azure SQL Database: {server}/{database}...{Colors.END}")
        
        try:
            if auth_type == 'token' and auth_config and 'token' in auth_config:
                # Token-based authentication
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database}"
                )
                conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': auth_config['token']})
                
            elif auth_type == 'sql' and auth_config and 'username' in auth_config:
                # SQL authentication
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={auth_config['username']};"
                    f"PWD={auth_config['password']}"
                )
                conn = pyodbc.connect(conn_str)
                
            else:
                # Interactive authentication (default)
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Authentication=ActiveDirectoryInteractive"
                )
                conn = pyodbc.connect(conn_str)
            
            print(f"{Colors.GREEN}✅ Connected successfully to Azure SQL Database{Colors.END}")
            return conn
            
        except Exception as e:
            print(f"{Colors.RED}❌ Connection failed: {e}{Colors.END}")
            raise
    
    @staticmethod
    def connect_fabric_warehouse(workspace: str, warehouse: str, auth_config: Optional[Dict] = None) -> pyodbc.Connection:
        """
        Connect to Microsoft Fabric Warehouse
        
        Args:
            workspace: Fabric workspace name
            warehouse: Fabric warehouse name
            auth_config: Optional authentication configuration with keys:
                - 'token': Access token for token-based auth
                - 'auth_type': 'token' or 'interactive' (default: 'interactive')
        
        Returns:
            pyodbc.Connection: Database connection object
        
        Example:
            # Token-based authentication
            conn = ConnectionHelper.connect_fabric_warehouse(
                "myworkspace",
                "mywarehouse",
                {"token": access_token, "auth_type": "token"}
            )
            
            # Interactive authentication (default)
            conn = ConnectionHelper.connect_fabric_warehouse(
                "myworkspace",
                "mywarehouse"
            )
        """
        server = f"{workspace}.datawarehouse.fabric.microsoft.com"
        auth_type = auth_config.get('auth_type', 'interactive') if auth_config else 'interactive'
        
        print(f"{Colors.BLUE}Connecting to Fabric Warehouse: {server}/{warehouse}...{Colors.END}")
        
        try:
            if auth_type == 'token' and auth_config and 'token' in auth_config:
                # Token-based authentication
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={warehouse}"
                )
                conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': auth_config['token']})
                
            else:
                # Interactive authentication (default)
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={warehouse};"
                    f"Authentication=ActiveDirectoryInteractive"
                )
                conn = pyodbc.connect(conn_str)
            
            print(f"{Colors.GREEN}✅ Connected successfully to Fabric Warehouse{Colors.END}")
            return conn
            
        except Exception as e:
            print(f"{Colors.RED}❌ Connection failed: {e}{Colors.END}")
            raise
    
    @staticmethod
    def get_spark_token(resource: str = "https://analysis.windows.net/powerbi/api"):
        """
        Get authentication token using Fabric notebook utilities
        
        Args:
            resource: Resource URL for token request
        
        Returns:
            str: Access token
        
        Note:
            This function uses notebookutils which is only available in Fabric notebooks
        """
        try:
            from notebookutils import mssparkutils
            token = mssparkutils.credentials.getToken(resource)
            print(f"{Colors.GREEN}✅ Token acquired from Fabric runtime{Colors.END}")
            return token
        except ImportError:
            print(f"{Colors.YELLOW}⚠️  notebookutils not available. Use this function only in Fabric notebooks.{Colors.END}")
            return None


class MigrationUtils:
    """Utility functions for migration operations"""
    
    @staticmethod
    def setup_external_objects(conn: pyodbc.Connection, storage_account: str, container: str) -> bool:
        """
        Setup external objects (credential, data source, file format) for migration
        
        Args:
            conn: Database connection
            storage_account: Azure storage account name
            container: Container name
        
        Returns:
            bool: True if successful, False otherwise
        """
        print(f"{Colors.BLUE}Setting up external objects...{Colors.END}")
        
        cursor = conn.cursor()
        
        try:
            # Create database scoped credential using managed identity
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'MigrationCredential')
                BEGIN
                    CREATE DATABASE SCOPED CREDENTIAL MigrationCredential
                    WITH IDENTITY = 'Managed Identity';
                END
            """)
            print(f"{Colors.GREEN}✅ Database scoped credential created{Colors.END}")
            
            # Create external data source
            location = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'MigrationStaging')
                BEGIN
                    CREATE EXTERNAL DATA SOURCE MigrationStaging
                    WITH (
                        TYPE = HADOOP,
                        LOCATION = '{location}',
                        CREDENTIAL = MigrationCredential
                    );
                END
            """)
            print(f"{Colors.GREEN}✅ External data source created{Colors.END}")
            
            # Create external file format
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFormat')
                BEGIN
                    CREATE EXTERNAL FILE FORMAT ParquetFormat
                    WITH (
                        FORMAT_TYPE = PARQUET,
                        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
                    );
                END
            """)
            print(f"{Colors.GREEN}✅ External file format created{Colors.END}")
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to setup external objects: {e}{Colors.END}")
            return False
    
    @staticmethod
    def get_tables_list(conn: pyodbc.Connection) -> List[Tuple[str, str, int, float]]:
        """
        Get list of tables from database with metadata
        
        Args:
            conn: Database connection
        
        Returns:
            List of tuples: (schema_name, table_name, row_count, size_gb)
        """
        print(f"{Colors.BLUE}Discovering tables...{Colors.END}")
        
        cursor = conn.cursor()
        
        # Try Synapse-specific query first
        try:
            query = """
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                SUM(ps.row_count) as row_count,
                SUM(ps.reserved_page_count) * 8.0 / 1024 / 1024 as size_gb
            FROM sys.dm_pdw_nodes_db_partition_stats ps
            INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id AND ps.pdw_node_id = nt.pdw_node_id
            INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
            INNER JOIN sys.tables t ON tm.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'migration')
            GROUP BY s.name, t.name
            HAVING SUM(ps.row_count) > 0
            ORDER BY size_gb DESC
            """
            cursor.execute(query)
            tables = cursor.fetchall()
        except:
            # Fallback to standard SQL query for non-Synapse databases
            query = """
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                SUM(p.rows) as row_count,
                SUM(a.total_pages) * 8.0 / 1024 / 1024 as size_gb
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'migration')
                AND p.index_id IN (0,1)
            GROUP BY s.name, t.name
            HAVING SUM(p.rows) > 0
            ORDER BY size_gb DESC
            """
            cursor.execute(query)
            tables = cursor.fetchall()
        
        print(f"{Colors.GREEN}✅ Found {len(tables)} tables{Colors.END}")
        
        # Display summary
        if len(tables) > 0:
            total_size = sum(t[3] for t in tables)
            total_rows = sum(t[2] for t in tables)
            
            print(f"\n{Colors.BOLD}Summary:{Colors.END}")
            print(f"  Total tables: {len(tables)}")
            print(f"  Total rows: {total_rows:,}")
            print(f"  Total size: {total_size:.2f} GB\n")
            
            # Show top 10 largest tables
            print(f"{Colors.BOLD}Top 10 largest tables:{Colors.END}")
            for i, (schema, table, rows, size_gb) in enumerate(tables[:10], 1):
                print(f"  {i:2d}. [{schema}].[{table}]: {rows:,} rows, {size_gb:.2f} GB")
            
            if len(tables) > 10:
                print(f"  ... and {len(tables) - 10} more tables")
            print()
        
        return tables
    
    @staticmethod
    def log_operation(operation: str, status: str, details: str = ""):
        """
        Log migration operation with timestamp and colored output
        
        Args:
            operation: Operation name
            status: Status ('success', 'failed', 'warning', 'info')
            details: Additional details
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if status == 'success':
            color = Colors.GREEN
            icon = "✅"
        elif status == 'failed':
            color = Colors.RED
            icon = "❌"
        elif status == 'warning':
            color = Colors.YELLOW
            icon = "⚠️"
        else:
            color = Colors.BLUE
            icon = "ℹ️"
        
        print(f"{color}{icon} [{timestamp}] {operation}{Colors.END}")
        if details:
            print(f"   {details}")
    
    @staticmethod
    def validate_row_count(source_conn: pyodbc.Connection, target_conn: pyodbc.Connection,
                          schema: str, table: str) -> Dict:
        """
        Validate row count between source and target tables
        
        Args:
            source_conn: Source database connection
            target_conn: Target database connection
            schema: Schema name
            table: Table name
        
        Returns:
            Dict with validation results
        """
        try:
            # Get source count
            source_cursor = source_conn.cursor()
            source_cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            source_count = source_cursor.fetchone()[0]
            
            # Get target count
            target_cursor = target_conn.cursor()
            target_cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            target_count = target_cursor.fetchone()[0]
            
            match = source_count == target_count
            
            return {
                'status': 'success' if match else 'mismatch',
                'source_count': source_count,
                'target_count': target_count,
                'difference': abs(source_count - target_count),
                'match': match
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }


class StorageHelper:
    """Helper class for Azure Data Lake Storage operations"""
    
    @staticmethod
    def get_adls_path(storage_account: str, container: str, path: str = "") -> str:
        """
        Construct ADLS Gen2 path
        
        Args:
            storage_account: Storage account name
            container: Container name
            path: Optional path within container
        
        Returns:
            str: Full ADLS path
        """
        base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        if path:
            return f"{base_path}/{path}"
        return base_path
    
    @staticmethod
    def read_parquet_with_spark(spark, storage_account: str, container: str, path: str):
        """
        Read parquet files from ADLS using Spark
        
        Args:
            spark: SparkSession object
            storage_account: Storage account name
            container: Container name
            path: Path to parquet files
        
        Returns:
            DataFrame: Spark DataFrame
        """
        adls_path = StorageHelper.get_adls_path(storage_account, container, path)
        print(f"{Colors.BLUE}Reading parquet from: {adls_path}{Colors.END}")
        
        try:
            df = spark.read.parquet(adls_path)
            print(f"{Colors.GREEN}✅ Successfully read {df.count()} rows{Colors.END}")
            return df
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to read parquet: {e}{Colors.END}")
            raise
    
    @staticmethod
    def write_parquet_with_spark(df, storage_account: str, container: str, path: str, mode: str = "overwrite"):
        """
        Write DataFrame to parquet in ADLS using Spark
        
        Args:
            df: Spark DataFrame
            storage_account: Storage account name
            container: Container name
            path: Path to write parquet files
            mode: Write mode ('overwrite', 'append', etc.)
        """
        adls_path = StorageHelper.get_adls_path(storage_account, container, path)
        print(f"{Colors.BLUE}Writing parquet to: {adls_path}{Colors.END}")
        
        try:
            df.write.mode(mode).parquet(adls_path)
            print(f"{Colors.GREEN}✅ Successfully written parquet{Colors.END}")
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to write parquet: {e}{Colors.END}")
            raise
