# Migration Notebooks - Quick Reference

## Overview

This directory contains PySpark notebooks for migrating data from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse. The notebooks provide an interactive, step-by-step approach to running migration operations directly in Fabric.

## Key Features

### 🎯 Helper Functions Module (`utils/migration_helpers.py`)

**ConnectionHelper Class:**
- `connect_azure_sql()` - Connect to Azure SQL Database or Synapse Dedicated SQL Pool
  - Supports token-based authentication (managed identity)
  - Supports interactive authentication (browser-based)
  - Supports SQL authentication
- `connect_fabric_warehouse()` - Connect to Microsoft Fabric Warehouse
  - Optimized for Fabric token-based authentication
  - Fallback to interactive authentication
- `get_spark_token()` - Get authentication token from Fabric runtime

**MigrationUtils Class:**
- `setup_external_objects()` - Setup credential, data source, and file format
- `get_tables_list()` - Discover tables with row counts and sizes
- `validate_row_count()` - Compare row counts between source and target
- `log_operation()` - Structured logging with colored output

**StorageHelper Class:**
- `get_adls_path()` - Construct ADLS Gen2 paths
- `read_parquet_with_spark()` - Read Parquet files using Spark
- `write_parquet_with_spark()` - Write Parquet files using Spark

### 📓 Interactive Notebooks

**01_extract_data.ipynb** - Data Extraction
- Connects to Azure Synapse
- Discovers all tables automatically
- Extracts data to ADLS in Parquet format using CETAS
- Organized by schema/table folder structure
- Progress tracking with colored output

**02_load_data.ipynb** - Data Loading
- Connects to Fabric Warehouse
- Discovers tables in ADLS
- Creates schemas and tables automatically
- Loads data using COPY INTO with retry logic
- Optional row count validation
- Updates statistics for optimal performance

**03_validate_migration.ipynb** - Validation
- Compares source and target databases
- Validates row counts for all tables
- Identifies missing or extra tables
- Generates detailed HTML validation report
- Clear pass/fail indicators

## Quick Start

### 1. Upload to Fabric

Upload files to your Fabric Lakehouse:
- `utils/migration_helpers.py` → `/Files/notebooks/utils/`
- All `*.ipynb` files → Import into Fabric Notebooks

### 2. Configure

Update configuration cells in each notebook with your environment details:
```python
# Example configuration
source_server = "mysynapse.sql.azuresynapse.net"
source_database = "mydatabase"
target_workspace = "myworkspace"
target_warehouse = "mywarehouse"
storage_account = "mystorageaccount"
container = "migration-staging"
auth_type = 'token'  # Use Fabric managed identity
```

### 3. Run Sequentially

1. **Extract**: Run `01_extract_data.ipynb` to export data to ADLS
2. **Load**: Run `02_load_data.ipynb` to import data into Fabric
3. **Validate**: Run `03_validate_migration.ipynb` to verify migration

## Authentication

### Token-Based (Recommended)
```python
auth_type = 'token'  # Uses Fabric managed identity
```
- No credentials needed
- Seamless in Fabric environment
- More secure

### Interactive
```python
auth_type = 'interactive'  # Opens browser
```
- Good for testing
- Requires user interaction

## Example Usage

### Connect to Azure SQL Database
```python
from migration_helpers import ConnectionHelper

token = ConnectionHelper.get_spark_token("https://database.windows.net/.default")
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase",
    {"auth_type": "token", "token": token}
)
```

### Connect to Fabric Warehouse
```python
from migration_helpers import ConnectionHelper

token = ConnectionHelper.get_spark_token("https://analysis.windows.net/powerbi/api")
conn = ConnectionHelper.connect_fabric_warehouse(
    "myworkspace",
    "mywarehouse",
    {"auth_type": "token", "token": token}
)
```

### Setup External Objects
```python
from migration_helpers import MigrationUtils

success = MigrationUtils.setup_external_objects(
    conn,
    "mystorageaccount",
    "migration-staging"
)
```

### Discover Tables
```python
from migration_helpers import MigrationUtils

tables = MigrationUtils.get_tables_list(conn)
# Returns: [(schema, table, row_count, size_gb), ...]
```

### Validate Row Counts
```python
from migration_helpers import MigrationUtils

result = MigrationUtils.validate_row_count(
    source_conn,
    target_conn,
    "dbo",
    "customers"
)

if result['match']:
    print(f"✓ Row counts match: {result['source_count']:,} rows")
else:
    print(f"✗ Mismatch: source={result['source_count']:,}, target={result['target_count']:,}")
```

### Work with ADLS
```python
from migration_helpers import StorageHelper

# Construct ADLS path
path = StorageHelper.get_adls_path("mystorageaccount", "container", "schema/table")
# Returns: abfss://container@mystorageaccount.dfs.core.windows.net/schema/table

# Read Parquet
df = StorageHelper.read_parquet_with_spark(spark, "mystorageaccount", "container", "dbo/customers")

# Write Parquet
StorageHelper.write_parquet_with_spark(df, "mystorageaccount", "container", "dbo/customers_transformed")
```

## Benefits Over Scripts

| Feature | Notebooks | Scripts |
|---------|-----------|---------|
| Environment | Fabric Notebooks | Local/VM/Shell |
| Interactivity | High (cell-by-cell) | Low (full script) |
| Debugging | Easy (inspect vars) | Standard (logs) |
| Authentication | Fabric managed | Multiple options |
| Visualization | Built-in outputs | Text only |
| Learning | Step-by-step | All-or-nothing |

## When to Use Notebooks

✅ **Use notebooks when:**
- Learning the migration process
- Running interactive exploration
- Small to medium datasets
- Want step-by-step control
- Need to inspect intermediate results
- Working in Fabric environment

❌ **Use scripts when:**
- Automated/scheduled migrations
- Large datasets (>1TB)
- Production environments
- Need parallelism across tables
- Running outside of Fabric

## Troubleshooting

### Import Errors
```python
# Verify path
import sys
print(sys.path)
# Should include: /lakehouse/default/Files/notebooks/utils
```

### Connection Issues
```python
# Test token acquisition
token = ConnectionHelper.get_spark_token("https://database.windows.net/.default")
print(f"Token acquired: {token is not None}")
```

### ADLS Access
```python
# Verify path construction
from migration_helpers import StorageHelper
path = StorageHelper.get_adls_path("account", "container", "path")
print(f"ADLS path: {path}")
```

## Additional Resources

- [Complete Notebook Documentation](README.md)
- [Migration Guide](../MIGRATION_GUIDE.md)
- [Permissions Guide](../PERMISSIONS_GUIDE.md)
- [Data Type Mapping](../DATATYPE_MAPPING.md)
- [Python Scripts Alternative](../scripts/README.md)

## Support

For issues:
1. Check [notebooks/README.md](README.md) for detailed docs
2. Review cell outputs for specific errors
3. Verify permissions and connectivity
4. Check helper function imports
5. Open an issue with error details
