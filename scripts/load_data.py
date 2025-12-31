#!/usr/bin/env python3
"""
load_data.py

Load data from Azure Data Lake Storage Gen2 to Microsoft Fabric Warehouse
using COPY INTO approach.

This script:
1. Connects to target Fabric Warehouse
2. Discovers extracted tables in ADLS
3. Creates schemas and tables
4. Loads data using COPY INTO with parallel processing
5. Validates row counts
6. Updates statistics
7. Logs progress and errors

Usage:
    python3 load_data.py \
        --workspace myworkspace \
        --warehouse mywarehouse \
        --storage-account mystorageaccount \
        --container migration-staging \
        --parallel-jobs 8 \
        --validate-rows
"""

import argparse
import pyodbc
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Tuple
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import os

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'

class DataLoader:
    def __init__(self, config: Dict):
        """Initialize data loader with configuration"""
        self.config = config
        self.credential = self._get_credential()
        self.conn = None
        self.source_conn = None  # For validation
        self.stats = {
            'total_tables': 0,
            'loaded': 0,
            'failed': 0,
            'validated': 0,
            'validation_failed': 0,
            'start_time': None,
            'end_time': None
        }
        
    def _get_credential(self):
        """Get Azure credential (managed identity or service principal)"""
        if os.getenv('AZURE_CLIENT_ID') and os.getenv('AZURE_CLIENT_SECRET'):
            return ClientSecretCredential(
                tenant_id=os.getenv('AZURE_TENANT_ID'),
                client_id=os.getenv('AZURE_CLIENT_ID'),
                client_secret=os.getenv('AZURE_CLIENT_SECRET')
            )
        return DefaultAzureCredential()
    
    def connect(self):
        """Connect to target Fabric Warehouse"""
        server = f"{self.config['workspace']}.datawarehouse.fabric.microsoft.com"
        print(f"{Colors.BLUE}Connecting to Fabric Warehouse: {server}...{Colors.END}")
        
        try:
            # Try with managed identity/service principal
            token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={server};"
                       f"DATABASE={self.config['warehouse']}")
            self.conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            print(f"{Colors.GREEN}✅ Connected successfully{Colors.END}\n")
            return True
        except Exception as e:
            # Fallback to interactive authentication
            print(f"{Colors.YELLOW}Token auth failed, trying interactive...{Colors.END}")
            try:
                conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                           f"SERVER={server};"
                           f"DATABASE={self.config['warehouse']};"
                           f"Authentication=ActiveDirectoryInteractive")
                self.conn = pyodbc.connect(conn_str)
                print(f"{Colors.GREEN}✅ Connected successfully{Colors.END}\n")
                return True
            except Exception as e2:
                print(f"{Colors.RED}❌ Connection failed: {e2}{Colors.END}")
                return False
    
    def connect_source(self):
        """Connect to source database for validation (optional)"""
        if not self.config.get('validate_rows', False):
            return True
            
        if not self.config.get('source_server') or not self.config.get('source_database'):
            print(f"{Colors.YELLOW}⚠️  Source connection info not provided, skipping row count validation{Colors.END}")
            return True
        
        print(f"{Colors.BLUE}Connecting to source for validation...{Colors.END}")
        
        try:
            token = self.credential.get_token("https://database.windows.net/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={self.config['source_server']};"
                       f"DATABASE={self.config['source_database']}")
            self.source_conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            print(f"{Colors.GREEN}✅ Source connection established{Colors.END}\n")
            return True
        except Exception as e:
            print(f"{Colors.YELLOW}⚠️  Could not connect to source: {e}{Colors.END}")
            print(f"{Colors.YELLOW}    Row count validation will be skipped{Colors.END}\n")
            return True
    
    def setup_external_objects(self):
        """Create database scoped credential, external data source, and file format"""
        print(f"{Colors.BLUE}Setting up external objects in Fabric Warehouse...{Colors.END}")
        
        cursor = self.conn.cursor()
        
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
            location = f"abfss://{self.config['container']}@{self.config['storage_account']}.dfs.core.windows.net"
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
                        FORMAT_TYPE = PARQUET
                    );
                END
            """)
            print(f"{Colors.GREEN}✅ External file format created{Colors.END}\n")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to setup external objects: {e}{Colors.END}")
            return False
    
    def discover_tables_in_storage(self) -> List[Tuple[str, str]]:
        """Discover tables in ADLS storage based on folder structure"""
        print(f"{Colors.BLUE}Discovering tables in storage...{Colors.END}")
        
        try:
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.config['storage_account']}.blob.core.windows.net",
                credential=self.credential
            )
            
            container_client = blob_service_client.get_container_client(self.config['container'])
            
            # Get all blob paths
            blobs = container_client.list_blobs()
            
            # Extract unique schema/table combinations
            tables = set()
            for blob in blobs:
                # Expected format: schema/table/file.parquet
                parts = blob.name.split('/')
                if len(parts) >= 2:
                    schema = parts[0]
                    table = parts[1]
                    if schema and table:
                        tables.add((schema, table))
            
            tables = sorted(list(tables))
            print(f"{Colors.GREEN}✅ Found {len(tables)} tables in storage{Colors.END}\n")
            
            # Display tables
            if len(tables) > 0:
                print(f"{Colors.BOLD}Tables to load:{Colors.END}")
                for i, (schema, table) in enumerate(tables[:20], 1):
                    print(f"  {i:2d}. [{schema}].[{table}]")
                if len(tables) > 20:
                    print(f"  ... and {len(tables) - 20} more tables")
                print()
            
            self.stats['total_tables'] = len(tables)
            return tables
            
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to discover tables: {e}{Colors.END}")
            return []
    
    def create_schema(self, schema: str) -> bool:
        """Create schema if not exists"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
                BEGIN
                    EXEC('CREATE SCHEMA [{schema}]')
                END
            """)
            self.conn.commit()
            return True
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to create schema {schema}: {e}{Colors.END}")
            return False
    
    def get_table_schema(self, schema: str, table: str) -> str:
        """Get table schema from source database (if connected)"""
        if not self.source_conn:
            # Default schema if source not available
            return "CREATE TABLE [{schema}].[{table}] (data NVARCHAR(MAX))"
        
        try:
            cursor = self.source_conn.cursor()
            cursor.execute(f"""
                SELECT 
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tbl ON c.object_id = tbl.object_id
                INNER JOIN sys.schemas s ON tbl.schema_id = s.schema_id
                WHERE s.name = '{schema}' AND tbl.name = '{table}'
                ORDER BY c.column_id
            """)
            
            columns = cursor.fetchall()
            if not columns:
                return None
            
            # Build CREATE TABLE statement
            col_defs = []
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                max_length = col[2]
                precision = col[3]
                scale = col[4]
                is_nullable = col[5]
                
                # Format data type
                if data_type in ('varchar', 'char', 'nvarchar', 'nchar', 'binary', 'varbinary'):
                    if max_length == -1:
                        # MAX types - limit to Fabric max
                        if data_type in ('varchar', 'varbinary'):
                            type_str = f"{data_type}(8000)"
                        else:
                            type_str = f"{data_type}(4000)"
                    else:
                        if data_type in ('nvarchar', 'nchar'):
                            type_str = f"{data_type}({max_length // 2})"
                        else:
                            type_str = f"{data_type}({max_length})"
                elif data_type in ('decimal', 'numeric'):
                    type_str = f"{data_type}({precision},{scale})"
                elif data_type == 'datetime':
                    type_str = "DATETIME2(3)"
                elif data_type == 'smalldatetime':
                    type_str = "DATETIME2(0)"
                elif data_type == 'money':
                    type_str = "DECIMAL(19,4)"
                elif data_type == 'smallmoney':
                    type_str = "DECIMAL(10,4)"
                else:
                    type_str = data_type
                
                nullable_str = "NULL" if is_nullable else "NOT NULL"
                col_defs.append(f"[{col_name}] {type_str} {nullable_str}")
            
            create_stmt = f"CREATE TABLE [{schema}].[{table}] (\n    " + ",\n    ".join(col_defs) + "\n)"
            return create_stmt
            
        except Exception as e:
            print(f"{Colors.YELLOW}⚠️  Could not get schema for [{schema}].[{table}]: {e}{Colors.END}")
            return None
    
    def load_table(self, schema: str, table: str) -> Dict:
        """Load a single table using COPY INTO"""
        start_time = time.time()
        
        try:
            # Create connection for this thread
            server = f"{self.config['workspace']}.datawarehouse.fabric.microsoft.com"
            token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={server};"
                       f"DATABASE={self.config['warehouse']}")
            conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            cursor = conn.cursor()
            
            # Create schema if not exists
            cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
                BEGIN
                    EXEC('CREATE SCHEMA [{schema}]')
                END
            """)
            
            # Check if table exists, drop if it does
            cursor.execute(f"""
                IF EXISTS (SELECT * FROM sys.tables t
                          INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                          WHERE s.name = '{schema}' AND t.name = '{table}')
                BEGIN
                    DROP TABLE [{schema}].[{table}]
                END
            """)
            
            # Get table schema from source
            create_stmt = self.get_table_schema(schema, table)
            
            if create_stmt:
                # Create table with proper schema
                cursor.execute(create_stmt)
            else:
                # Fallback: Let COPY INTO infer schema
                pass
            
            # Load data using COPY INTO
            location = f"{schema}/{table}/"
            
            # Retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    cursor.execute(f"""
                        COPY INTO [{schema}].[{table}]
                        FROM '{location}'
                        WITH (
                            DATA_SOURCE = 'MigrationStaging',
                            FILE_TYPE = 'PARQUET',
                            MAXERRORS = 10000,
                            ERRORFILE = 'errors/{schema}/{table}/'
                        )
                    """)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        time.sleep(wait_time)
                    else:
                        raise e
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            row_count = cursor.fetchone()[0]
            
            conn.commit()
            conn.close()
            
            duration = time.time() - start_time
            
            return {
                'status': 'success',
                'schema': schema,
                'table': table,
                'row_count': row_count,
                'duration': duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            return {
                'status': 'failed',
                'schema': schema,
                'table': table,
                'row_count': 0,
                'duration': duration,
                'error': str(e)
            }
    
    def validate_row_count(self, schema: str, table: str, target_count: int) -> Dict:
        """Validate row count against source"""
        if not self.source_conn:
            return {'status': 'skipped'}
        
        try:
            cursor = self.source_conn.cursor()
            cursor.execute(f"""
                SELECT SUM(ps.row_count)
                FROM sys.dm_pdw_nodes_db_partition_stats ps
                INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id AND ps.pdw_node_id = nt.pdw_node_id
                INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
                INNER JOIN sys.tables t ON tm.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = '{schema}' AND t.name = '{table}'
            """)
            
            source_count = cursor.fetchone()[0] or 0
            
            match = source_count == target_count
            
            return {
                'status': 'validated' if match else 'mismatch',
                'source_count': source_count,
                'target_count': target_count,
                'difference': abs(source_count - target_count)
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def load_tables_parallel(self, tables: List[Tuple], max_workers: int = 4):
        """Load tables in parallel"""
        print(f"{Colors.BLUE}Starting parallel loading with {max_workers} workers...{Colors.END}\n")
        
        self.stats['start_time'] = datetime.now()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(self.load_table, schema, table): (schema, table)
                for schema, table in tables
            }
            
            # Process completed tasks
            for future in as_completed(future_to_table):
                result = future.result()
                
                if result['status'] == 'success':
                    self.stats['loaded'] += 1
                    print(f"{Colors.GREEN}✅ [{result['schema']}].[{result['table']}]{Colors.END}")
                    print(f"   {result['row_count']:,} rows loaded, {result['duration']:.1f}s")
                    
                    # Validate row count if enabled
                    if self.config.get('validate_rows', False) and self.source_conn:
                        validation = self.validate_row_count(
                            result['schema'], 
                            result['table'], 
                            result['row_count']
                        )
                        if validation['status'] == 'validated':
                            self.stats['validated'] += 1
                            print(f"   {Colors.GREEN}✓ Row count validated{Colors.END}")
                        elif validation['status'] == 'mismatch':
                            self.stats['validation_failed'] += 1
                            print(f"   {Colors.RED}✗ Row count mismatch: source={validation['source_count']:,}, target={validation['target_count']:,}{Colors.END}")
                    
                    print(f"   Progress: {self.stats['loaded']}/{self.stats['total_tables']} tables\n")
                else:
                    self.stats['failed'] += 1
                    print(f"{Colors.RED}❌ [{result['schema']}].[{result['table']}]{Colors.END}")
                    print(f"   Error: {result['error']}\n")
        
        self.stats['end_time'] = datetime.now()
    
    def update_statistics(self):
        """Update statistics on all loaded tables"""
        print(f"{Colors.BLUE}Updating statistics...{Colors.END}")
        
        try:
            cursor = self.conn.cursor()
            
            # Get all user tables
            cursor.execute("""
                SELECT s.name, t.name
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            """)
            
            tables = cursor.fetchall()
            
            for schema, table in tables:
                try:
                    cursor.execute(f"UPDATE STATISTICS [{schema}].[{table}]")
                    print(f"   ✓ [{schema}].[{table}]")
                except Exception as e:
                    print(f"   {Colors.YELLOW}⚠️  Failed for [{schema}].[{table}]: {e}{Colors.END}")
            
            self.conn.commit()
            print(f"{Colors.GREEN}✅ Statistics updated{Colors.END}\n")
            
        except Exception as e:
            print(f"{Colors.YELLOW}⚠️  Failed to update statistics: {e}{Colors.END}\n")
    
    def print_summary(self):
        """Print loading summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*70)
        print(f"{Colors.BOLD}LOADING SUMMARY{Colors.END}")
        print("="*70)
        print(f"Total tables:       {self.stats['total_tables']}")
        print(f"Loaded:             {Colors.GREEN}{self.stats['loaded']}{Colors.END}")
        print(f"Failed:             {Colors.RED}{self.stats['failed']}{Colors.END}")
        
        if self.config.get('validate_rows', False):
            print(f"Validated:          {Colors.GREEN}{self.stats['validated']}{Colors.END}")
            print(f"Validation failed:  {Colors.RED}{self.stats['validation_failed']}{Colors.END}")
        
        print(f"Duration:           {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"Start time:         {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time:           {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        if self.stats['failed'] > 0:
            print(f"{Colors.YELLOW}⚠️  Some tables failed to load. Check logs above for details.{Colors.END}")
        elif self.stats['validation_failed'] > 0:
            print(f"{Colors.YELLOW}⚠️  Some tables have row count mismatches. Verify data integrity.{Colors.END}")
        else:
            print(f"{Colors.GREEN}✅ All tables loaded successfully!{Colors.END}")
    
    def run(self):
        """Run the complete loading process"""
        print("\n" + "="*70)
        print(f"{Colors.BOLD}DATA LOADING FROM ADLS TO FABRIC WAREHOUSE{Colors.END}")
        print("="*70 + "\n")
        
        # Connect to target
        if not self.connect():
            return False
        
        # Connect to source (for validation)
        if not self.connect_source():
            return False
        
        # Setup external objects
        if not self.setup_external_objects():
            return False
        
        # Discover tables
        tables = self.discover_tables_in_storage()
        
        if len(tables) == 0:
            print(f"{Colors.YELLOW}⚠️  No tables found in storage{Colors.END}")
            return True
        
        # Load tables in parallel
        max_workers = self.config.get('parallel_jobs', 4)
        self.load_tables_parallel(tables, max_workers)
        
        # Update statistics
        if self.config.get('update_stats', True):
            self.update_statistics()
        
        # Print summary
        self.print_summary()
        
        # Close connections
        if self.conn:
            self.conn.close()
        if self.source_conn:
            self.source_conn.close()
        
        return self.stats['failed'] == 0 and self.stats['validation_failed'] == 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Load data from ADLS Gen2 to Fabric Warehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic loading
  python3 load_data.py \\
      --workspace myworkspace \\
      --warehouse mywarehouse \\
      --storage-account mystorageaccount \\
      --container migration-staging
  
  # Parallel loading with validation
  python3 load_data.py \\
      --workspace myworkspace \\
      --warehouse mywarehouse \\
      --storage-account mystorageaccount \\
      --container migration-staging \\
      --parallel-jobs 8 \\
      --validate-rows \\
      --source-server mysynapse.sql.azuresynapse.net \\
      --source-database mydatabase
        """
    )
    
    parser.add_argument('--workspace', required=True,
                       help='Fabric workspace name')
    parser.add_argument('--warehouse', required=True,
                       help='Fabric warehouse name')
    parser.add_argument('--storage-account', required=True,
                       help='Storage account name')
    parser.add_argument('--container', required=True,
                       help='Container name for staging')
    parser.add_argument('--parallel-jobs', type=int, default=4,
                       help='Number of parallel loading jobs (default: 4)')
    parser.add_argument('--validate-rows', action='store_true',
                       help='Validate row counts against source')
    parser.add_argument('--source-server',
                       help='Source server for validation (required if --validate-rows)')
    parser.add_argument('--source-database',
                       help='Source database for validation (required if --validate-rows)')
    parser.add_argument('--update-stats', action='store_true', default=True,
                       help='Update statistics after loading (default: True)')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'workspace': args.workspace,
        'warehouse': args.warehouse,
        'storage_account': args.storage_account,
        'container': args.container,
        'parallel_jobs': args.parallel_jobs,
        'validate_rows': args.validate_rows,
        'source_server': args.source_server,
        'source_database': args.source_database,
        'update_stats': args.update_stats
    }
    
    # Run loading
    loader = DataLoader(config)
    success = loader.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
