# Migration Scripts ETL Library Documentation

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Components](#core-components)
- [Installation & Setup](#installation--setup)
- [Python ETL Scripts](#python-etl-scripts)
  - [DataExtractor](#dataextractor)
  - [DataLoader](#dataloader)
  - [MigrationValidator](#migrationvalidator)
- [PySpark Utilities](#pyspark-utilities)
  - [ConnectionHelper](#connectionhelper)
  - [MigrationUtils](#migrationutils)
  - [StorageHelper](#storagehelper)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Overview

The **Migration Scripts ETL Library** is a comprehensive Python-based toolkit for migrating data from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse. The library provides both standalone Python scripts and PySpark notebook utilities for flexible deployment options.

### Key Features

- ✅ **Extract**: Pull data from Azure Synapse to ADLS Gen2 using CETAS (CREATE EXTERNAL TABLE AS SELECT)
- ✅ **Load**: Push data from ADLS Gen2 to Fabric Warehouse using COPY INTO
- ✅ **Validate**: Compare row counts and data integrity between source and target
- ✅ **Parallel Processing**: Concurrent table operations for optimal performance
- ✅ **Auto-Retry Logic**: Built-in exponential backoff for transient failures
- ✅ **Authentication**: Support for Managed Identity, Service Principal, and Interactive auth
- ✅ **Monitoring**: Colored console output with progress tracking
- ✅ **Error Handling**: Comprehensive error capture and reporting

### Architecture Overview

```
┌─────────────────────┐
│  Azure Synapse      │
│  Dedicated Pool     │
└──────────┬──────────┘
           │ Extract (CETAS)
           ↓
┌─────────────────────┐
│  Azure Data Lake    │
│  Storage Gen2       │
│  (Parquet Files)    │
└──────────┬──────────┘
           │ Load (COPY INTO)
           ↓
┌─────────────────────┐
│  Microsoft Fabric   │
│  Warehouse          │
└─────────────────────┘
```

---

## Architecture

### Design Patterns

1. **Class-Based Architecture**: Each ETL component is encapsulated in a dedicated class
2. **Configuration-Driven**: All settings passed via configuration dictionaries
3. **Thread Pool Execution**: Parallel processing using `concurrent.futures.ThreadPoolExecutor`
4. **Credential Management**: Centralized authentication via Azure Identity library
5. **Separation of Concerns**: Clear separation between extraction, loading, and validation

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Runtime** | Python 3.7+ | Core execution environment |
| **Database Driver** | pyodbc | SQL Server connectivity |
| **Authentication** | azure.identity | Azure credential management |
| **Storage** | azure.storage.blob | ADLS Gen2 operations |
| **Concurrency** | concurrent.futures | Parallel execution |
| **Notebooks** | PySpark | Fabric notebook support |

---

## Core Components

### 1. DataExtractor (`extract_data.py`)

Extracts data from Azure Synapse Dedicated SQL Pool to Azure Data Lake Storage Gen2 using the CETAS approach.

**Key Capabilities:**
- Discovers all tables in source database
- Creates external data sources and file formats
- Exports data to Parquet format with Snappy compression
- Supports intelligent partitioning for large tables (>1GB)
- Parallel extraction with configurable worker count
- Progress tracking and error logging

**Class Hierarchy:**
```
DataExtractor
├── _get_credential()      # Authentication
├── connect()              # Database connection
├── setup_external_objects() # External data source setup
├── get_tables_to_extract() # Table discovery
├── extract_table()        # Single table extraction
├── extract_tables_parallel() # Parallel orchestration
└── print_summary()        # Results reporting
```

### 2. DataLoader (`load_data.py`)

Loads data from Azure Data Lake Storage Gen2 to Microsoft Fabric Warehouse using COPY INTO.

**Key Capabilities:**
- Discovers extracted tables in ADLS Gen2
- Auto-creates schemas and tables
- Loads data with COPY INTO statement
- Row count validation against source
- Statistics update post-load
- Retry logic with exponential backoff

**Class Hierarchy:**
```
DataLoader
├── _get_credential()         # Authentication
├── connect()                 # Warehouse connection
├── connect_source()          # Optional source connection
├── setup_external_objects()  # External data source setup
├── discover_tables_in_storage() # ADLS table discovery
├── create_schema()           # Schema creation
├── get_table_schema()        # DDL generation
├── load_table()              # Single table load
├── validate_row_count()      # Row count validation
├── load_tables_parallel()    # Parallel orchestration
├── update_statistics()       # Post-load optimization
└── print_summary()           # Results reporting
```

### 3. MigrationValidator (`validate_migration.py`)

Validates migration completeness by comparing source and target databases.

**Key Capabilities:**
- Row count comparison for all tables
- Missing table detection
- Data type compatibility checks
- Detailed validation reports
- Success/failure status tracking

**Class Hierarchy:**
```
MigrationValidator
├── connect_source()          # Source connection
├── connect_target()          # Target connection
├── get_source_tables()       # Source table discovery
├── get_target_tables()       # Target table discovery
├── validate_row_counts()     # Row count comparison
├── generate_report()         # Report generation
└── run()                     # Orchestration
```

---

## Installation & Setup

### Prerequisites

1. **Python 3.7 or higher**
2. **ODBC Driver 17 for SQL Server**
3. **Azure CLI** (for authentication)

### Installation Steps

```bash
# 1. Clone the repository
cd /home/runner/work/fabric-migration_v1/fabric-migration_v1

# 2. Install dependencies
pip install -r scripts/requirements.txt

# 3. Verify ODBC driver installation
odbcinst -j

# 4. Login to Azure
az login
```

### Required Python Packages

```txt
pyodbc>=4.0.35
azure-identity>=1.12.0
azure-storage-blob>=12.19.0
```

### Environment Variables (Optional)

For Service Principal authentication:

```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

---

## Python ETL Scripts

### DataExtractor

#### Basic Usage

```python
from extract_data import DataExtractor

config = {
    'server': 'mysynapse.sql.azuresynapse.net',
    'database': 'mydatabase',
    'storage_account': 'mystorageaccount',
    'container': 'migration-staging',
    'parallel_jobs': 6,
    'enable_partitioning': True
}

extractor = DataExtractor(config)
success = extractor.run()
```

#### Command Line Usage

```bash
# Basic extraction
python3 scripts/extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging

# Parallel extraction with 8 workers
python3 scripts/extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8
```

#### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `server` | str | Yes | - | Synapse server FQDN |
| `database` | str | Yes | - | Database name |
| `storage_account` | str | Yes | - | ADLS Gen2 storage account |
| `container` | str | Yes | - | Container for staging |
| `parallel_jobs` | int | No | 4 | Number of concurrent extractions |
| `enable_partitioning` | bool | No | True | Enable partitioning for large tables |

#### Key Methods

##### `connect()`
Establishes connection to source Synapse database.

**Returns:** `bool` - True if successful

**Authentication Flow:**
1. Attempts token-based auth (Managed Identity/Service Principal)
2. Falls back to interactive authentication if token fails

**Example:**
```python
extractor = DataExtractor(config)
if extractor.connect():
    print("Connected successfully")
```

##### `setup_external_objects()`
Creates master key, credential, external data source, and file format.

**Creates:**
- Database master key (if not exists)
- Database scoped credential (Managed Identity)
- External data source pointing to ADLS Gen2
- Parquet file format with Snappy compression

**Returns:** `bool` - True if successful

##### `get_tables_to_extract()`
Discovers all tables in source database with metadata.

**Returns:** `List[Tuple[str, str, int, float]]` - List of (schema, table, row_count, size_gb)

**Query Logic:**
- Uses Synapse-specific DMVs (`sys.dm_pdw_nodes_db_partition_stats`)
- Filters out system schemas
- Orders by size (largest first)
- Excludes empty tables

##### `extract_table(schema: str, table: str, row_count: int, size_gb: float)`
Extracts a single table using CETAS.

**Parameters:**
- `schema`: Schema name
- `table`: Table name
- `row_count`: Expected row count
- `size_gb`: Table size in GB

**Returns:** `Dict` with status, duration, and error (if failed)

**Logic:**
- Creates external table with naming convention `ext_{table}_migration`
- Uses partitioning for tables >1GB (configurable)
- Writes Parquet files to `{schema}/{table}/` path
- Includes retry logic and error capture

##### `extract_tables_parallel(tables: List[Tuple], max_workers: int)`
Orchestrates parallel extraction of multiple tables.

**Parameters:**
- `tables`: List of tables to extract
- `max_workers`: Number of concurrent workers

**Features:**
- ThreadPoolExecutor for parallel execution
- Real-time progress tracking
- Per-table success/failure logging
- Statistics collection

#### Output Format

Extracted data is stored in ADLS Gen2 with this structure:

```
container/
├── schema1/
│   ├── table1/
│   │   ├── part-00000.parquet
│   │   ├── part-00001.parquet
│   │   └── ...
│   └── table2/
│       └── part-00000.parquet
└── schema2/
    └── table3/
        └── part-00000.parquet
```

---

### DataLoader

#### Basic Usage

```python
from load_data import DataLoader

config = {
    'workspace': 'myworkspace',
    'warehouse': 'mywarehouse',
    'storage_account': 'mystorageaccount',
    'container': 'migration-staging',
    'parallel_jobs': 8,
    'validate_rows': True,
    'source_server': 'mysynapse.sql.azuresynapse.net',
    'source_database': 'mydatabase',
    'update_stats': True
}

loader = DataLoader(config)
success = loader.run()
```

#### Command Line Usage

```bash
# Basic loading
python3 scripts/load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging

# Loading with validation
python3 scripts/load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --update-stats
```

#### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `workspace` | str | Yes | - | Fabric workspace name |
| `warehouse` | str | Yes | - | Fabric warehouse name |
| `storage_account` | str | Yes | - | ADLS Gen2 storage account |
| `container` | str | Yes | - | Container with staged data |
| `parallel_jobs` | int | No | 4 | Number of concurrent loads |
| `validate_rows` | bool | No | False | Enable row count validation |
| `source_server` | str | No | - | Source server for validation |
| `source_database` | str | No | - | Source database for validation |
| `update_stats` | bool | No | True | Update statistics post-load |

#### Key Methods

##### `connect()`
Connects to target Fabric Warehouse.

**Connection String Format:**
```
{workspace}.datawarehouse.fabric.microsoft.com
```

**Token Resource:**
```
https://analysis.windows.net/powerbi/api/.default
```

##### `discover_tables_in_storage()`
Scans ADLS Gen2 to find all extracted tables.

**Returns:** `List[Tuple[str, str]]` - List of (schema, table)

**Logic:**
- Uses Azure Storage Blob SDK
- Parses folder structure: `{schema}/{table}/*.parquet`
- Returns unique schema/table combinations

##### `get_table_schema(schema: str, table: str)`
Retrieves table DDL from source database.

**Returns:** `str` - CREATE TABLE statement

**Features:**
- Auto-converts incompatible data types
- Handles MAX types (varchar(MAX) → varchar(8000))
- Converts datetime → datetime2
- Converts money → decimal

**Type Mapping:**
| Source Type | Target Type |
|-------------|-------------|
| `varchar(MAX)` | `varchar(8000)` |
| `nvarchar(MAX)` | `nvarchar(4000)` |
| `datetime` | `datetime2(3)` |
| `smalldatetime` | `datetime2(0)` |
| `money` | `decimal(19,4)` |
| `smallmoney` | `decimal(10,4)` |

##### `load_table(schema: str, table: str)`
Loads a single table using COPY INTO.

**Returns:** `Dict` with status, row_count, duration, and error (if failed)

**Logic:**
1. Create schema (if not exists)
2. Drop existing table (if exists)
3. Create table with proper schema
4. Execute COPY INTO with retry logic
5. Get row count
6. Return results

**COPY INTO Options:**
```sql
COPY INTO [schema].[table]
FROM '{schema}/{table}/'
WITH (
    DATA_SOURCE = 'MigrationStaging',
    FILE_TYPE = 'PARQUET',
    MAXERRORS = 10000,
    ERRORFILE = 'errors/{schema}/{table}/'
)
```

##### `validate_row_count(schema: str, table: str, target_count: int)`
Validates row count against source.

**Returns:** `Dict` with status, source_count, target_count, difference

**Validation Logic:**
- Queries source using Synapse DMVs
- Compares with target count
- Returns match status and difference

##### `update_statistics()`
Updates statistics on all loaded tables.

**SQL Command:**
```sql
UPDATE STATISTICS [schema].[table]
```

**Features:**
- Runs on all user tables
- Continues on error (logs warning)
- Essential for query performance

---

### MigrationValidator

#### Basic Usage

```python
from validate_migration import MigrationValidator

config = {
    'source_server': 'mysynapse.sql.azuresynapse.net',
    'source_database': 'mydatabase',
    'target_workspace': 'myworkspace',
    'target_warehouse': 'mywarehouse',
    'generate_report': True
}

validator = MigrationValidator(config)
success = validator.run()
```

#### Command Line Usage

```bash
python3 scripts/validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

#### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source_server` | str | Yes | - | Source Synapse server |
| `source_database` | str | Yes | - | Source database |
| `target_workspace` | str | Yes | - | Target Fabric workspace |
| `target_warehouse` | str | Yes | - | Target Fabric warehouse |
| `generate_report` | bool | No | False | Generate detailed report |

#### Key Methods

##### `validate_row_counts(source_tables: Dict, target_tables: Dict)`
Compares row counts between source and target.

**Validation Categories:**
1. **Matches**: Row counts are identical
2. **Mismatches**: Row counts differ
3. **Missing in Target**: Table exists in source but not target
4. **Extra in Target**: Table exists in target but not source

**Output:**
- Console output with colored status indicators
- Detailed per-table comparison
- Summary statistics

##### `generate_report()`
Creates a detailed validation report file.

**Report Format:**
```
==================================================
MIGRATION VALIDATION REPORT
==================================================

Generated: 2024-01-15 10:30:00
Source: mysynapse.sql.azuresynapse.net / mydatabase
Target: myworkspace / mywarehouse

--------------------------------------------------
ROW COUNT VALIDATION
--------------------------------------------------

Total tables validated: 50
Matches: 48
Mismatches: 2
Missing in target: 0
Extra in target: 0

Tables with row count mismatches:
--------------------------------------------------
schema1.table1
  Source: 1,000,000 rows
  Target: 999,999 rows
  Difference: 1 rows (0.00%)

schema2.table2
  Source: 500,000 rows
  Target: 500,100 rows
  Difference: 100 rows (0.02%)

==================================================
END OF REPORT
==================================================
```

**File Naming:**
```
migration_validation_report_{timestamp}.txt
```

---

## PySpark Utilities

The `notebooks/utils/migration_helpers.py` module provides helper classes for use in Fabric PySpark notebooks.

### ConnectionHelper

Helper class for establishing database connections in PySpark notebooks.

#### Methods

##### `connect_azure_sql(server: str, database: str, auth_config: Optional[Dict])`
Connects to Azure SQL Database or Synapse.

**Example:**
```python
from migration_helpers import ConnectionHelper

# Interactive authentication
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase"
)

# Token-based authentication
token = mssparkutils.credentials.getToken("https://database.windows.net/.default")
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase",
    {"token": token, "auth_type": "token"}
)

# SQL authentication
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase",
    {
        "username": "sqladmin",
        "password": "P@ssw0rd",
        "auth_type": "sql"
    }
)
```

##### `connect_fabric_warehouse(workspace: str, warehouse: str, auth_config: Optional[Dict])`
Connects to Microsoft Fabric Warehouse.

**Example:**
```python
# Interactive authentication
conn = ConnectionHelper.connect_fabric_warehouse(
    "myworkspace",
    "mywarehouse"
)

# Token-based authentication (in Fabric notebook)
token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api/.default")
conn = ConnectionHelper.connect_fabric_warehouse(
    "myworkspace",
    "mywarehouse",
    {"token": token, "auth_type": "token"}
)
```

##### `get_spark_token(resource: str)`
Acquires authentication token using Fabric notebook utilities.

**Example:**
```python
# Get token for Fabric Warehouse
token = ConnectionHelper.get_spark_token("https://analysis.windows.net/powerbi/api")

# Get token for Azure SQL
token = ConnectionHelper.get_spark_token("https://database.windows.net")
```

**Note:** Only available in Fabric notebook environment.

---

### MigrationUtils

Utility functions for common migration operations.

#### Methods

##### `setup_external_objects(conn: pyodbc.Connection, storage_account: str, container: str)`
Creates external objects required for migration.

**Example:**
```python
from migration_helpers import ConnectionHelper, MigrationUtils

conn = ConnectionHelper.connect_fabric_warehouse("myworkspace", "mywarehouse")

success = MigrationUtils.setup_external_objects(
    conn,
    "mystorageaccount",
    "migration-staging"
)

if success:
    print("External objects created successfully")
```

##### `get_tables_list(conn: pyodbc.Connection)`
Retrieves list of tables with metadata.

**Returns:** `List[Tuple[str, str, int, float]]` - (schema, table, row_count, size_gb)

**Example:**
```python
conn = ConnectionHelper.connect_azure_sql("myserver.database.windows.net", "mydatabase")

tables = MigrationUtils.get_tables_list(conn)

for schema, table, row_count, size_gb in tables:
    print(f"[{schema}].[{table}]: {row_count:,} rows, {size_gb:.2f} GB")
```

##### `log_operation(operation: str, status: str, details: str)`
Logs migration operation with colored output.

**Example:**
```python
MigrationUtils.log_operation("Extract table", "success", "1000000 rows extracted")
MigrationUtils.log_operation("Load table", "failed", "Connection timeout")
MigrationUtils.log_operation("Validation", "warning", "Row count mismatch")
MigrationUtils.log_operation("Setup", "info", "Creating external objects")
```

##### `validate_row_count(source_conn, target_conn, schema: str, table: str)`
Validates row count between source and target.

**Returns:** `Dict` with validation results

**Example:**
```python
source_conn = ConnectionHelper.connect_azure_sql("mysynapse.sql.azuresynapse.net", "mydb")
target_conn = ConnectionHelper.connect_fabric_warehouse("myworkspace", "mywarehouse")

result = MigrationUtils.validate_row_count(
    source_conn,
    target_conn,
    "dbo",
    "sales"
)

if result['match']:
    print(f"✅ Validation passed: {result['source_count']:,} rows")
else:
    print(f"❌ Mismatch: source={result['source_count']:,}, target={result['target_count']:,}")
```

---

### StorageHelper

Helper class for Azure Data Lake Storage operations.

#### Methods

##### `get_adls_path(storage_account: str, container: str, path: str)`
Constructs ADLS Gen2 path.

**Example:**
```python
from migration_helpers import StorageHelper

path = StorageHelper.get_adls_path(
    "mystorageaccount",
    "migration-staging",
    "dbo/sales"
)
# Returns: abfss://migration-staging@mystorageaccount.dfs.core.windows.net/dbo/sales
```

##### `read_parquet_with_spark(spark, storage_account: str, container: str, path: str)`
Reads Parquet files from ADLS using Spark.

**Example:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = StorageHelper.read_parquet_with_spark(
    spark,
    "mystorageaccount",
    "migration-staging",
    "dbo/sales"
)

df.show(10)
print(f"Total rows: {df.count()}")
```

##### `write_parquet_with_spark(df, storage_account: str, container: str, path: str, mode: str)`
Writes DataFrame to Parquet in ADLS.

**Example:**
```python
# Write with overwrite mode
StorageHelper.write_parquet_with_spark(
    df,
    "mystorageaccount",
    "migration-staging",
    "dbo/sales_backup",
    mode="overwrite"
)

# Write with append mode
StorageHelper.write_parquet_with_spark(
    df,
    "mystorageaccount",
    "migration-staging",
    "dbo/sales_incremental",
    mode="append"
)
```

---

## Usage Examples

### Complete Migration Workflow

```bash
# Step 1: Setup environment
cd scripts
./setup_environment.sh

# Step 2: Run pre-migration checks
./pre_migration_checks.sh

# Step 3: Extract data from Synapse
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6

# Step 4: Load data to Fabric Warehouse
python3 load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase

# Step 5: Validate migration
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

### Python Script Integration

```python
#!/usr/bin/env python3
"""Complete migration orchestration"""

from extract_data import DataExtractor
from load_data import DataLoader
from validate_migration import MigrationValidator

def run_migration():
    # Configuration
    source_config = {
        'server': 'mysynapse.sql.azuresynapse.net',
        'database': 'mydatabase',
        'storage_account': 'mystorageaccount',
        'container': 'migration-staging',
        'parallel_jobs': 6
    }
    
    target_config = {
        'workspace': 'myworkspace',
        'warehouse': 'mywarehouse',
        'storage_account': 'mystorageaccount',
        'container': 'migration-staging',
        'parallel_jobs': 8,
        'validate_rows': True,
        'source_server': source_config['server'],
        'source_database': source_config['database']
    }
    
    validation_config = {
        'source_server': source_config['server'],
        'source_database': source_config['database'],
        'target_workspace': target_config['workspace'],
        'target_warehouse': target_config['warehouse'],
        'generate_report': True
    }
    
    # Step 1: Extract
    print("Step 1: Extracting data from Synapse...")
    extractor = DataExtractor(source_config)
    if not extractor.run():
        print("❌ Extraction failed")
        return False
    
    # Step 2: Load
    print("\nStep 2: Loading data to Fabric Warehouse...")
    loader = DataLoader(target_config)
    if not loader.run():
        print("❌ Loading failed")
        return False
    
    # Step 3: Validate
    print("\nStep 3: Validating migration...")
    validator = MigrationValidator(validation_config)
    if not validator.run():
        print("⚠️  Validation found issues - check report")
        return False
    
    print("\n✅ Migration completed successfully!")
    return True

if __name__ == "__main__":
    import sys
    success = run_migration()
    sys.exit(0 if success else 1)
```

### PySpark Notebook Example

```python
# Cell 1: Setup
from migration_helpers import ConnectionHelper, MigrationUtils, StorageHelper
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Configuration
SOURCE_SERVER = "mysynapse.sql.azuresynapse.net"
SOURCE_DATABASE = "mydatabase"
TARGET_WORKSPACE = "myworkspace"
TARGET_WAREHOUSE = "mywarehouse"
STORAGE_ACCOUNT = "mystorageaccount"
CONTAINER = "migration-staging"

# Cell 2: Connect to source
source_token = mssparkutils.credentials.getToken("https://database.windows.net/.default")
source_conn = ConnectionHelper.connect_azure_sql(
    SOURCE_SERVER,
    SOURCE_DATABASE,
    {"token": source_token, "auth_type": "token"}
)

# Cell 3: Get tables to migrate
tables = MigrationUtils.get_tables_list(source_conn)
print(f"Found {len(tables)} tables to migrate")

# Cell 4: Extract sample table
schema, table, row_count, size_gb = tables[0]
print(f"Extracting [{schema}].[{table}]: {row_count:,} rows, {size_gb:.2f} GB")

# Read from source (using Spark JDBC)
df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{SOURCE_SERVER}") \
    .option("dbtable", f"[{schema}].[{table}]") \
    .option("accessToken", source_token) \
    .load()

# Write to ADLS
StorageHelper.write_parquet_with_spark(
    df,
    STORAGE_ACCOUNT,
    CONTAINER,
    f"{schema}/{table}"
)

# Cell 5: Load to Fabric Warehouse
target_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api/.default")
target_conn = ConnectionHelper.connect_fabric_warehouse(
    TARGET_WORKSPACE,
    TARGET_WAREHOUSE,
    {"token": target_token, "auth_type": "token"}
)

# Setup external objects
MigrationUtils.setup_external_objects(target_conn, STORAGE_ACCOUNT, CONTAINER)

# Execute COPY INTO
cursor = target_conn.cursor()
cursor.execute(f"""
    COPY INTO [{schema}].[{table}]
    FROM '{schema}/{table}/'
    WITH (
        DATA_SOURCE = 'MigrationStaging',
        FILE_TYPE = 'PARQUET'
    )
""")
target_conn.commit()

# Cell 6: Validate
result = MigrationUtils.validate_row_count(source_conn, target_conn, schema, table)

if result['match']:
    MigrationUtils.log_operation("Validation", "success", f"{result['source_count']:,} rows matched")
else:
    MigrationUtils.log_operation("Validation", "failed", 
        f"Source: {result['source_count']:,}, Target: {result['target_count']:,}")

# Cell 7: Cleanup
source_conn.close()
target_conn.close()
```

---

## API Reference

### Colors Class

ANSI color codes for terminal output.

**Attributes:**
```python
Colors.GREEN    # '\033[92m'
Colors.YELLOW   # '\033[93m'
Colors.RED      # '\033[91m'
Colors.BLUE     # '\033[94m'
Colors.END      # '\033[0m'
Colors.BOLD     # '\033[1m'
```

**Usage:**
```python
print(f"{Colors.GREEN}Success!{Colors.END}")
print(f"{Colors.RED}Error!{Colors.END}")
print(f"{Colors.BOLD}Important{Colors.END}")
```

### Authentication Methods

All classes use Azure Identity library for authentication with this priority:

1. **Environment Variables** (Service Principal)
   - `AZURE_TENANT_ID`
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`

2. **DefaultAzureCredential** (Managed Identity)
   - System-assigned managed identity
   - User-assigned managed identity

3. **Interactive Authentication** (Fallback)
   - Azure CLI login
   - Browser-based authentication

### Error Handling

All methods include comprehensive error handling:

```python
try:
    # Operation
    result = operation()
except Exception as e:
    print(f"{Colors.RED}❌ Operation failed: {e}{Colors.END}")
    return {
        'status': 'failed',
        'error': str(e)
    }
```

### Retry Logic

COPY INTO operations include exponential backoff:

```python
max_retries = 3
for attempt in range(max_retries):
    try:
        cursor.execute(copy_into_statement)
        break
    except Exception as e:
        if attempt < max_retries - 1:
            wait_time = 2 ** attempt  # 1s, 2s, 4s
            time.sleep(wait_time)
        else:
            raise e
```

---

## Best Practices

### Performance Optimization

1. **Parallel Jobs Configuration**
   ```python
   # Small databases (<10GB): 2-4 workers
   parallel_jobs = 3
   
   # Medium databases (10-100GB): 4-8 workers
   parallel_jobs = 6
   
   # Large databases (>100GB): 8-16 workers
   parallel_jobs = 12
   ```

2. **Partitioning Strategy**
   - Enable partitioning for tables >1GB
   - Default partition count: `min(int(size_gb) + 1, 10)`
   - Maximum 10 partitions per table

3. **Batch Processing**
   - Process tables in order of size (largest first)
   - Allows smaller tables to finish while large tables run
   - Better resource utilization

### Security Best Practices

1. **Managed Identity** (Recommended)
   ```python
   # No credentials in code
   credential = DefaultAzureCredential()
   ```

2. **Service Principal** (Automation)
   ```bash
   export AZURE_TENANT_ID="..."
   export AZURE_CLIENT_ID="..."
   export AZURE_CLIENT_SECRET="..."
   ```

3. **Avoid** SQL authentication in production

### Monitoring & Logging

1. **Enable Progress Tracking**
   ```python
   # All scripts include built-in progress tracking
   extractor.run()  # Automatically logs progress
   ```

2. **Generate Validation Reports**
   ```bash
   python3 validate_migration.py \
       --source-server ... \
       --target-workspace ... \
       --generate-report  # Creates detailed report file
   ```

3. **Check Statistics**
   ```python
   # After running, check stats dictionary
   print(f"Total: {extractor.stats['total_tables']}")
   print(f"Success: {extractor.stats['extracted']}")
   print(f"Failed: {extractor.stats['failed']}")
   ```

### Error Recovery

1. **Idempotent Operations**
   - All scripts can be re-run safely
   - Extract: Drops/recreates external tables
   - Load: Drops/recreates target tables

2. **Failed Table Retry**
   ```bash
   # Re-run will only process tables that failed
   python3 extract_data.py --server ... --database ...
   ```

3. **Incremental Loading**
   ```python
   # Filter tables to load specific ones
   tables_to_retry = [('dbo', 'failed_table1'), ('dbo', 'failed_table2')]
   loader.load_tables_parallel(tables_to_retry, max_workers=4)
   ```

---

## Troubleshooting

### Common Issues

#### 1. Connection Failures

**Problem:** `Failed to connect to database`

**Solutions:**
```bash
# Check ODBC driver
odbcinst -j

# Install ODBC driver 17
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Verify Azure authentication
az login
az account show
```

#### 2. Token Expiration

**Problem:** `Token has expired`

**Solutions:**
```python
# Refresh Azure CLI token
os.system("az login")

# Use Service Principal with auto-refresh
from azure.identity import ClientSecretCredential

credential = ClientSecretCredential(
    tenant_id=os.getenv('AZURE_TENANT_ID'),
    client_id=os.getenv('AZURE_CLIENT_ID'),
    client_secret=os.getenv('AZURE_CLIENT_SECRET')
)
```

#### 3. COPY INTO Failures

**Problem:** `COPY INTO failed with error`

**Solutions:**
```sql
-- Check error files
SELECT * FROM sys.dm_external_load_errors
WHERE load_start_time > DATEADD(hour, -1, GETDATE())

-- Verify external data source
SELECT * FROM sys.external_data_sources

-- Verify file format
SELECT * FROM sys.external_file_formats

-- Check file permissions
-- Ensure Managed Identity has "Storage Blob Data Reader" role
```

#### 4. Row Count Mismatches

**Problem:** `Row count mismatch between source and target`

**Solutions:**
```sql
-- Check source row count (Synapse)
SELECT 
    s.name, t.name, SUM(ps.row_count) as row_count
FROM sys.dm_pdw_nodes_db_partition_stats ps
INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id
INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
INNER JOIN sys.tables t ON tm.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name = 'my_table'
GROUP BY s.name, t.name

-- Check target row count (Fabric)
SELECT COUNT(*) FROM [dbo].[my_table]

-- Check for errors during COPY INTO
SELECT * FROM sys.dm_external_load_errors
WHERE load_start_time > DATEADD(hour, -1, GETDATE())
```

#### 5. Memory Issues

**Problem:** `Out of memory during parallel operations`

**Solutions:**
```python
# Reduce parallel workers
config = {
    ...
    'parallel_jobs': 2  # Reduce from 8 to 2
}

# Process tables in batches
tables = get_tables_to_extract()
batch_size = 10

for i in range(0, len(tables), batch_size):
    batch = tables[i:i+batch_size]
    extractor.extract_tables_parallel(batch, max_workers=2)
```

#### 6. Storage Permission Issues

**Problem:** `Failed to access storage account`

**Solutions:**
```bash
# Check storage account permissions
az storage account show --name mystorageaccount

# Grant "Storage Blob Data Reader" to Managed Identity
az role assignment create \
    --role "Storage Blob Data Reader" \
    --assignee <managed-identity-object-id> \
    --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>

# Verify role assignment
az role assignment list \
    --assignee <managed-identity-object-id> \
    --scope /subscriptions/<subscription-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>
```

### Debug Mode

Enable detailed logging:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Run scripts with debug output
extractor = DataExtractor(config)
extractor.run()
```

### Performance Profiling

```python
import time
from datetime import datetime

def profile_operation(operation_name, func, *args, **kwargs):
    start_time = time.time()
    start_dt = datetime.now()
    
    print(f"Starting {operation_name} at {start_dt}")
    
    result = func(*args, **kwargs)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Completed {operation_name} in {duration:.2f} seconds")
    
    return result

# Usage
profile_operation("Data Extraction", extractor.run)
profile_operation("Data Loading", loader.run)
```

---

## Appendix

### File Structure

```
fabric-migration_v1/
├── scripts/
│   ├── extract_data.py           # DataExtractor class
│   ├── load_data.py              # DataLoader class
│   ├── validate_migration.py     # MigrationValidator class
│   ├── requirements.txt          # Python dependencies
│   ├── setup_environment.sh      # Environment setup script
│   └── pre_migration_checks.sh   # Pre-migration validation
├── notebooks/
│   ├── utils/
│   │   └── migration_helpers.py  # PySpark utility classes
│   ├── 01_extract_data.ipynb     # Extract notebook
│   ├── 02_load_data.ipynb        # Load notebook
│   └── 03_validate_migration.ipynb # Validate notebook
└── MSCLAKE_ETL_DOCUMENTATION.md  # This file
```

### Dependencies Version Matrix

| Package | Minimum Version | Recommended | Notes |
|---------|----------------|-------------|-------|
| Python | 3.7 | 3.10+ | async/await support |
| pyodbc | 4.0.30 | 4.0.39+ | SQL Server 2022 support |
| azure-identity | 1.10.0 | 1.15.0+ | Latest auth features |
| azure-storage-blob | 12.14.0 | 12.19.0+ | ADLS Gen2 support |

### Related Documentation

- [Migration Guide](MIGRATION_GUIDE.md)
- [Data Type Mapping](DATATYPE_MAPPING.md)
- [Permissions Guide](PERMISSIONS_GUIDE.md)
- [Quick Start](QUICK_START.md)
- [Best Practices](BEST_PRACTICES.md)

### Support Resources

- **GitHub Issues**: [Report issues](https://github.com/sandipan8609/fabric-migration_v1/issues)
- **Microsoft Docs**: [Fabric Warehouse Documentation](https://learn.microsoft.com/fabric/data-warehouse/)
- **Azure Synapse**: [Synapse Migration Guide](https://learn.microsoft.com/azure/synapse-analytics/)

---

**Document Version:** 1.0  
**Last Updated:** January 15, 2024  
**Author:** Migration Tools Team  
**License:** MIT

---

## Quick Reference Card

### Extract Data
```bash
python3 extract_data.py \
    --server <synapse-server> \
    --database <database> \
    --storage-account <storage> \
    --container <container> \
    --parallel-jobs 6
```

### Load Data
```bash
python3 load_data.py \
    --workspace <workspace> \
    --warehouse <warehouse> \
    --storage-account <storage> \
    --container <container> \
    --parallel-jobs 8 \
    --validate-rows \
    --source-server <synapse-server> \
    --source-database <database>
```

### Validate Migration
```bash
python3 validate_migration.py \
    --source-server <synapse-server> \
    --source-database <database> \
    --target-workspace <workspace> \
    --target-warehouse <warehouse> \
    --generate-report
```

### PySpark Helpers
```python
from migration_helpers import ConnectionHelper, MigrationUtils

# Connect
conn = ConnectionHelper.connect_fabric_warehouse("workspace", "warehouse")

# Setup
MigrationUtils.setup_external_objects(conn, "storage", "container")

# Get tables
tables = MigrationUtils.get_tables_list(conn)

# Validate
result = MigrationUtils.validate_row_count(source_conn, target_conn, "schema", "table")
```

---

**End of Documentation**
