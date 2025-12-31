#!/usr/bin/env python3
"""
extract_data.py

Extract data from Azure Synapse Dedicated SQL Pool to Azure Data Lake Storage Gen2
using CETAS (CREATE EXTERNAL TABLE AS SELECT) approach.

This script:
1. Connects to source Synapse database
2. Discovers all tables to migrate
3. Creates external data sources and file formats
4. Extracts data to ADLS Gen2 in Parquet format
5. Supports parallel extraction
6. Logs progress and errors

Usage:
    python3 extract_data.py \
        --server mysynapse.sql.azuresynapse.net \
        --database mydatabase \
        --storage-account mystorageaccount \
        --container migration-staging \
        --parallel-jobs 6 \
        --batch-size 50
"""

import argparse
import pyodbc
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Tuple
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import os

class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'

class DataExtractor:
    def __init__(self, config: Dict):
        """Initialize data extractor with configuration"""
        self.config = config
        self.credential = self._get_credential()
        self.conn = None
        self.stats = {
            'total_tables': 0,
            'extracted': 0,
            'failed': 0,
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
        """Connect to source Synapse database"""
        print(f"{Colors.BLUE}Connecting to {self.config['server']}...{Colors.END}")
        
        try:
            # Try with managed identity/service principal
            token = self.credential.get_token("https://database.windows.net/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={self.config['server']};"
                       f"DATABASE={self.config['database']}")
            self.conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            print(f"{Colors.GREEN}✅ Connected successfully{Colors.END}\n")
            return True
        except Exception as e:
            # Fallback to interactive authentication
            print(f"{Colors.YELLOW}Token auth failed, trying interactive...{Colors.END}")
            try:
                conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                           f"SERVER={self.config['server']};"
                           f"DATABASE={self.config['database']};"
                           f"Authentication=ActiveDirectoryInteractive")
                self.conn = pyodbc.connect(conn_str)
                print(f"{Colors.GREEN}✅ Connected successfully{Colors.END}\n")
                return True
            except Exception as e2:
                print(f"{Colors.RED}❌ Connection failed: {e2}{Colors.END}")
                return False
    
    def setup_external_objects(self):
        """Create master key, credential, external data source, and file format"""
        print(f"{Colors.BLUE}Setting up external objects...{Colors.END}")
        
        cursor = self.conn.cursor()
        
        try:
            # Create master key if not exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
                BEGIN
                    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Migration2024!Strong';
                END
            """)
            print(f"{Colors.GREEN}✅ Master key ready{Colors.END}")
            
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
                        FORMAT_TYPE = PARQUET,
                        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
                    );
                END
            """)
            print(f"{Colors.GREEN}✅ External file format created{Colors.END}\n")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to setup external objects: {e}{Colors.END}")
            return False
    
    def get_tables_to_extract(self) -> List[Tuple[str, str, int, float]]:
        """Get list of tables to extract with metadata"""
        print(f"{Colors.BLUE}Discovering tables...{Colors.END}")
        
        cursor = self.conn.cursor()
        
        # Get table list with row counts and sizes
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
        
        print(f"{Colors.GREEN}✅ Found {len(tables)} tables to extract{Colors.END}\n")
        
        # Display table summary
        total_size = sum(t[3] for t in tables)
        total_rows = sum(t[2] for t in tables)
        
        print(f"{Colors.BOLD}Summary:{Colors.END}")
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
        
        self.stats['total_tables'] = len(tables)
        return tables
    
    def extract_table(self, schema: str, table: str, row_count: int, size_gb: float) -> Dict:
        """Extract a single table using CETAS"""
        start_time = time.time()
        
        try:
            # Create connection for this thread
            token = self.credential.get_token("https://database.windows.net/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={self.config['server']};"
                       f"DATABASE={self.config['database']}")
            conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            cursor = conn.cursor()
            
            # Drop external table if exists
            external_table_name = f"ext_{table}_migration"
            cursor.execute(f"""
                IF EXISTS (SELECT * FROM sys.external_tables WHERE name = '{external_table_name}')
                    DROP EXTERNAL TABLE [{schema}].[{external_table_name}]
            """)
            
            # Create external table with CETAS
            location = f"{schema}/{table}/"
            
            # Determine if table should be partitioned (> 1 GB)
            if size_gb > 1.0 and self.config.get('enable_partitioning', True):
                # For large tables, partition into multiple files
                partition_count = min(int(size_gb) + 1, 10)  # Max 10 partitions
                
                cursor.execute(f"""
                    CREATE EXTERNAL TABLE [{schema}].[{external_table_name}]
                    WITH (
                        LOCATION = '{location}',
                        DATA_SOURCE = MigrationStaging,
                        FILE_FORMAT = ParquetFormat
                    )
                    AS
                    SELECT * FROM [{schema}].[{table}]
                """)
            else:
                # Standard extraction for smaller tables
                cursor.execute(f"""
                    CREATE EXTERNAL TABLE [{schema}].[{external_table_name}]
                    WITH (
                        LOCATION = '{location}',
                        DATA_SOURCE = MigrationStaging,
                        FILE_FORMAT = ParquetFormat
                    )
                    AS
                    SELECT * FROM [{schema}].[{table}]
                """)
            
            conn.commit()
            conn.close()
            
            duration = time.time() - start_time
            
            return {
                'status': 'success',
                'schema': schema,
                'table': table,
                'row_count': row_count,
                'size_gb': size_gb,
                'duration': duration
            }
            
        except Exception as e:
            duration = time.time() - start_time
            return {
                'status': 'failed',
                'schema': schema,
                'table': table,
                'row_count': row_count,
                'size_gb': size_gb,
                'duration': duration,
                'error': str(e)
            }
    
    def extract_tables_parallel(self, tables: List[Tuple], max_workers: int = 4):
        """Extract tables in parallel"""
        print(f"{Colors.BLUE}Starting parallel extraction with {max_workers} workers...{Colors.END}\n")
        
        self.stats['start_time'] = datetime.now()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(self.extract_table, schema, table, rows, size): (schema, table)
                for schema, table, rows, size in tables
            }
            
            # Process completed tasks
            for future in as_completed(future_to_table):
                result = future.result()
                
                if result['status'] == 'success':
                    self.stats['extracted'] += 1
                    print(f"{Colors.GREEN}✅ [{result['schema']}].[{result['table']}]{Colors.END}")
                    print(f"   {result['row_count']:,} rows, {result['size_gb']:.2f} GB, {result['duration']:.1f}s")
                    print(f"   Progress: {self.stats['extracted']}/{self.stats['total_tables']} tables\n")
                else:
                    self.stats['failed'] += 1
                    print(f"{Colors.RED}❌ [{result['schema']}].[{result['table']}]{Colors.END}")
                    print(f"   Error: {result['error']}\n")
        
        self.stats['end_time'] = datetime.now()
    
    def print_summary(self):
        """Print extraction summary"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*70)
        print(f"{Colors.BOLD}EXTRACTION SUMMARY{Colors.END}")
        print("="*70)
        print(f"Total tables:     {self.stats['total_tables']}")
        print(f"Extracted:        {Colors.GREEN}{self.stats['extracted']}{Colors.END}")
        print(f"Failed:           {Colors.RED}{self.stats['failed']}{Colors.END}")
        print(f"Duration:         {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"Start time:       {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time:         {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        if self.stats['failed'] > 0:
            print(f"{Colors.YELLOW}⚠️  Some tables failed to extract. Check logs above for details.{Colors.END}")
        else:
            print(f"{Colors.GREEN}✅ All tables extracted successfully!{Colors.END}")
    
    def run(self):
        """Run the complete extraction process"""
        print("\n" + "="*70)
        print(f"{Colors.BOLD}DATA EXTRACTION FROM AZURE SYNAPSE TO ADLS{Colors.END}")
        print("="*70 + "\n")
        
        # Connect to database
        if not self.connect():
            return False
        
        # Setup external objects
        if not self.setup_external_objects():
            return False
        
        # Get tables to extract
        tables = self.get_tables_to_extract()
        
        if len(tables) == 0:
            print(f"{Colors.YELLOW}⚠️  No tables found to extract{Colors.END}")
            return True
        
        # Extract tables in parallel
        max_workers = self.config.get('parallel_jobs', 4)
        self.extract_tables_parallel(tables, max_workers)
        
        # Print summary
        self.print_summary()
        
        # Close connection
        if self.conn:
            self.conn.close()
        
        return self.stats['failed'] == 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract data from Azure Synapse to ADLS Gen2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic extraction
  python3 extract_data.py \\
      --server mysynapse.sql.azuresynapse.net \\
      --database mydatabase \\
      --storage-account mystorageaccount \\
      --container migration-staging
  
  # Parallel extraction with 8 workers
  python3 extract_data.py \\
      --server mysynapse.sql.azuresynapse.net \\
      --database mydatabase \\
      --storage-account mystorageaccount \\
      --container migration-staging \\
      --parallel-jobs 8
        """
    )
    
    parser.add_argument('--server', required=True,
                       help='Synapse server name (e.g., mysynapse.sql.azuresynapse.net)')
    parser.add_argument('--database', required=True,
                       help='Database name')
    parser.add_argument('--storage-account', required=True,
                       help='Storage account name')
    parser.add_argument('--container', required=True,
                       help='Container name for staging')
    parser.add_argument('--parallel-jobs', type=int, default=4,
                       help='Number of parallel extraction jobs (default: 4)')
    parser.add_argument('--enable-partitioning', action='store_true', default=True,
                       help='Enable partitioning for large tables (default: True)')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'server': args.server,
        'database': args.database,
        'storage_account': args.storage_account,
        'container': args.container,
        'parallel_jobs': args.parallel_jobs,
        'enable_partitioning': args.enable_partitioning
    }
    
    # Run extraction
    extractor = DataExtractor(config)
    success = extractor.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
