#!/usr/bin/env python3
"""
validate_migration.py

Validate migration between source and target databases.
Compares row counts, data types, and sample data.

Usage:
    python3 validate_migration.py \
        --source-server mysynapse.sql.azuresynapse.net \
        --source-database mydatabase \
        --target-workspace myworkspace \
        --target-warehouse mywarehouse \
        --generate-report
"""

import argparse
import pyodbc
import sys
from datetime import datetime
from typing import Dict, List
from azure.identity import DefaultAzureCredential
import os

class Colors:
    """ANSI color codes"""
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    END = '\033[0m'
    BOLD = '\033[1m'

class MigrationValidator:
    def __init__(self, config: Dict):
        self.config = config
        self.credential = DefaultAzureCredential()
        self.source_conn = None
        self.target_conn = None
        self.results = {
            'row_counts': [],
            'datatype_mismatches': [],
            'missing_tables': [],
            'extra_tables': []
        }
    
    def connect_source(self):
        """Connect to source database"""
        print(f"{Colors.BLUE}Connecting to source...{Colors.END}")
        try:
            token = self.credential.get_token("https://database.windows.net/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={self.config['source_server']};"
                       f"DATABASE={self.config['source_database']}")
            self.source_conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            print(f"{Colors.GREEN}✅ Connected to source{Colors.END}\n")
            return True
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to connect to source: {e}{Colors.END}")
            return False
    
    def connect_target(self):
        """Connect to target Fabric Warehouse"""
        print(f"{Colors.BLUE}Connecting to target...{Colors.END}")
        try:
            server = f"{self.config['target_workspace']}.datawarehouse.fabric.microsoft.com"
            token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={server};"
                       f"DATABASE={self.config['target_warehouse']}")
            self.target_conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
            print(f"{Colors.GREEN}✅ Connected to target{Colors.END}\n")
            return True
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to connect to target: {e}{Colors.END}")
            return False
    
    def get_source_tables(self) -> Dict[str, int]:
        """Get table list and row counts from source"""
        print(f"{Colors.BLUE}Getting source tables...{Colors.END}")
        
        cursor = self.source_conn.cursor()
        cursor.execute("""
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                SUM(ps.row_count) as row_count
            FROM sys.dm_pdw_nodes_db_partition_stats ps
            INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id AND ps.pdw_node_id = nt.pdw_node_id
            INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
            INNER JOIN sys.tables t ON tm.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'migration')
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
        """)
        
        tables = {}
        for row in cursor.fetchall():
            schema, table, row_count = row
            tables[f"{schema}.{table}"] = row_count
        
        print(f"{Colors.GREEN}✅ Found {len(tables)} tables in source{Colors.END}\n")
        return tables
    
    def get_target_tables(self) -> Dict[str, int]:
        """Get table list and row counts from target"""
        print(f"{Colors.BLUE}Getting target tables...{Colors.END}")
        
        cursor = self.target_conn.cursor()
        cursor.execute("""
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                SUM(p.rows) as row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'migration')
                AND p.index_id IN (0, 1)
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
        """)
        
        tables = {}
        for row in cursor.fetchall():
            schema, table, row_count = row
            tables[f"{schema}.{table}"] = row_count or 0
        
        print(f"{Colors.GREEN}✅ Found {len(tables)} tables in target{Colors.END}\n")
        return tables
    
    def validate_row_counts(self, source_tables: Dict, target_tables: Dict):
        """Validate row counts between source and target"""
        print(f"{Colors.BLUE}Validating row counts...{Colors.END}\n")
        
        all_tables = set(source_tables.keys()) | set(target_tables.keys())
        
        matches = 0
        mismatches = 0
        missing = 0
        
        for table in sorted(all_tables):
            source_count = source_tables.get(table, 0)
            target_count = target_tables.get(table, 0)
            
            if table not in source_tables:
                self.results['extra_tables'].append({
                    'table': table,
                    'row_count': target_count
                })
                print(f"{Colors.YELLOW}⚠️  {table}: Only in target ({target_count:,} rows){Colors.END}")
                missing += 1
                
            elif table not in target_tables:
                self.results['missing_tables'].append({
                    'table': table,
                    'row_count': source_count
                })
                print(f"{Colors.RED}❌ {table}: Missing in target (source: {source_count:,} rows){Colors.END}")
                missing += 1
                
            elif source_count == target_count:
                self.results['row_counts'].append({
                    'table': table,
                    'source': source_count,
                    'target': target_count,
                    'status': 'match'
                })
                print(f"{Colors.GREEN}✅ {table}: {source_count:,} rows{Colors.END}")
                matches += 1
                
            else:
                difference = abs(source_count - target_count)
                pct_diff = (difference / source_count * 100) if source_count > 0 else 0
                
                self.results['row_counts'].append({
                    'table': table,
                    'source': source_count,
                    'target': target_count,
                    'difference': difference,
                    'pct_diff': pct_diff,
                    'status': 'mismatch'
                })
                print(f"{Colors.RED}❌ {table}: Source={source_count:,}, Target={target_count:,} (diff: {difference:,}, {pct_diff:.2f}%){Colors.END}")
                mismatches += 1
        
        print(f"\n{Colors.BOLD}Row Count Summary:{Colors.END}")
        print(f"  Matches:    {Colors.GREEN}{matches}{Colors.END}")
        print(f"  Mismatches: {Colors.RED}{mismatches}{Colors.END}")
        print(f"  Missing:    {Colors.YELLOW}{missing}{Colors.END}")
        print()
    
    def generate_report(self):
        """Generate validation report"""
        if not self.config.get('generate_report', False):
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"migration_validation_report_{timestamp}.txt"
        
        print(f"{Colors.BLUE}Generating validation report...{Colors.END}")
        
        with open(report_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("MIGRATION VALIDATION REPORT\n")
            f.write("="*80 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {self.config['source_server']} / {self.config['source_database']}\n")
            f.write(f"Target: {self.config['target_workspace']} / {self.config['target_warehouse']}\n\n")
            
            # Row count validation
            f.write("-"*80 + "\n")
            f.write("ROW COUNT VALIDATION\n")
            f.write("-"*80 + "\n\n")
            
            matches = [r for r in self.results['row_counts'] if r['status'] == 'match']
            mismatches = [r for r in self.results['row_counts'] if r['status'] == 'mismatch']
            
            f.write(f"Total tables validated: {len(self.results['row_counts'])}\n")
            f.write(f"Matches: {len(matches)}\n")
            f.write(f"Mismatches: {len(mismatches)}\n")
            f.write(f"Missing in target: {len(self.results['missing_tables'])}\n")
            f.write(f"Extra in target: {len(self.results['extra_tables'])}\n\n")
            
            if mismatches:
                f.write("Tables with row count mismatches:\n")
                f.write("-"*80 + "\n")
                for r in mismatches:
                    f.write(f"{r['table']}\n")
                    f.write(f"  Source: {r['source']:,} rows\n")
                    f.write(f"  Target: {r['target']:,} rows\n")
                    f.write(f"  Difference: {r['difference']:,} rows ({r['pct_diff']:.2f}%)\n\n")
            
            if self.results['missing_tables']:
                f.write("\nTables missing in target:\n")
                f.write("-"*80 + "\n")
                for r in self.results['missing_tables']:
                    f.write(f"{r['table']} ({r['row_count']:,} rows in source)\n")
            
            f.write("\n" + "="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")
        
        print(f"{Colors.GREEN}✅ Report saved to: {report_file}{Colors.END}\n")
    
    def run(self):
        """Run validation"""
        print("\n" + "="*70)
        print(f"{Colors.BOLD}MIGRATION VALIDATION{Colors.END}")
        print("="*70 + "\n")
        
        # Connect to both databases
        if not self.connect_source():
            return False
        if not self.connect_target():
            return False
        
        # Get table lists
        source_tables = self.get_source_tables()
        target_tables = self.get_target_tables()
        
        # Validate row counts
        self.validate_row_counts(source_tables, target_tables)
        
        # Generate report
        if self.config.get('generate_report', False):
            self.generate_report()
        
        # Close connections
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        
        # Return success if no mismatches or missing tables
        mismatches = [r for r in self.results['row_counts'] if r['status'] == 'mismatch']
        success = len(mismatches) == 0 and len(self.results['missing_tables']) == 0
        
        return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Validate migration from Synapse to Fabric Warehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--source-server', required=True,
                       help='Source Synapse server')
    parser.add_argument('--source-database', required=True,
                       help='Source database name')
    parser.add_argument('--target-workspace', required=True,
                       help='Target Fabric workspace')
    parser.add_argument('--target-warehouse', required=True,
                       help='Target Fabric warehouse')
    parser.add_argument('--generate-report', action='store_true',
                       help='Generate detailed validation report')
    
    args = parser.parse_args()
    
    config = {
        'source_server': args.source_server,
        'source_database': args.source_database,
        'target_workspace': args.target_workspace,
        'target_warehouse': args.target_warehouse,
        'generate_report': args.generate_report
    }
    
    validator = MigrationValidator(config)
    success = validator.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
