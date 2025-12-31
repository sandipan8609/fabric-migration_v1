# Migration Notebooks for PySpark

This directory contains PySpark notebooks for migrating data from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse.

## Overview

These notebooks provide an interactive way to perform migration steps using PySpark in Fabric notebooks. They complement the Python scripts in the `/scripts` directory and are designed to run in Microsoft Fabric notebook environment.

## Notebooks

| Notebook | Purpose | Duration |
|----------|---------|----------|
| **01_extract_data.ipynb** | Extract data from Azure Synapse to ADLS Gen2 | 30-120 min |
| **02_load_data.ipynb** | Load data from ADLS to Fabric Warehouse | 30-120 min |
| **03_validate_migration.ipynb** | Validate migration completeness and accuracy | 10-30 min |

## Helper Functions

**`utils/migration_helpers.py`** - Shared utility module containing:
- `ConnectionHelper` - Database connection utilities
  - `connect_azure_sql()` - Connect to Azure SQL Database/Synapse
  - `connect_fabric_warehouse()` - Connect to Fabric Warehouse
  - `get_spark_token()` - Get authentication token from Fabric runtime
- `MigrationUtils` - Migration operation utilities
  - `setup_external_objects()` - Setup external data sources and formats
  - `get_tables_list()` - Discover tables with metadata
  - `validate_row_count()` - Compare row counts between databases
  - `log_operation()` - Structured logging with colors
- `StorageHelper` - ADLS storage operations
  - `get_adls_path()` - Construct ADLS paths
  - `read_parquet_with_spark()` - Read Parquet files using Spark
  - `write_parquet_with_spark()` - Write Parquet files using Spark

---

## Quick Start

### Prerequisites

1. **Microsoft Fabric Workspace** with:
   - Fabric Warehouse created
   - Lakehouse created (for storing notebooks and helper functions)
   - Appropriate permissions

2. **Azure Resources**:
   - Azure Synapse Dedicated SQL Pool (source)
   - Azure Data Lake Storage Gen2 (staging)
   - Service Principal or Managed Identity with appropriate permissions

3. **Required Permissions**: See [PERMISSIONS_GUIDE.md](../PERMISSIONS_GUIDE.md)

### Setup Instructions

#### Step 1: Upload Files to Fabric Lakehouse

1. Open your Fabric workspace and navigate to your Lakehouse
2. Upload the `utils/migration_helpers.py` file to `/Files/notebooks/utils/` in your Lakehouse
3. Upload all notebook files (*.ipynb) to `/Files/notebooks/` or import them into Fabric

#### Step 2: Configure Authentication

There are two primary authentication methods:

**Option A: Managed Identity (Recommended for Production)**

Use Fabric's built-in authentication with `auth_type = 'token'`:

```python
# In notebook configuration cell
auth_type = 'token'  # Uses Fabric managed identity
```

**Option B: Interactive Authentication (Development)**

Use interactive browser-based authentication:

```python
# In notebook configuration cell
auth_type = 'interactive'  # Opens browser for authentication
```

#### Step 3: Run Notebooks in Sequence

1. **Extract Data** (01_extract_data.ipynb)
   - Configure source and storage settings
   - Run all cells to extract tables to ADLS
   - Verify extraction completed successfully

2. **Load Data** (02_load_data.ipynb)
   - Configure target warehouse and storage settings
   - Run all cells to load tables into Fabric Warehouse
   - Verify loading completed successfully

3. **Validate Migration** (03_validate_migration.ipynb)
   - Configure source and target settings
   - Run all cells to validate row counts
   - Review validation report

---

## Detailed Notebook Guide

### 01_extract_data.ipynb

**Purpose**: Extract data from Azure Synapse to ADLS Gen2 in Parquet format

**Configuration Parameters**:
```python
source_server = "mysynapse.sql.azuresynapse.net"
source_database = "mydatabase"
storage_account = "mystorageaccount"
container = "migration-staging"
enable_partitioning = True
auth_type = 'token'  # or 'interactive'
```

**Process Flow**:
1. Connect to Azure Synapse
2. Setup external objects (credential, data source, file format)
3. Discover all tables to migrate
4. Extract tables using CREATE EXTERNAL TABLE AS SELECT (CETAS)
5. Data is written to ADLS in folder structure: `schema/table/*.parquet`

**Output**:
- Parquet files in ADLS organized by schema/table
- Extraction summary with success/failure counts
- Detailed progress logs

**Typical Runtime**: 30-120 minutes (depending on data volume and parallelism)

---

### 02_load_data.ipynb

**Purpose**: Load data from ADLS Gen2 to Fabric Warehouse

**Configuration Parameters**:
```python
target_workspace = "myworkspace"
target_warehouse = "mywarehouse"
storage_account = "mystorageaccount"
container = "migration-staging"
validate_row_counts = True
source_server = "mysynapse.sql.azuresynapse.net"  # Optional for validation
source_database = "mydatabase"  # Optional for validation
update_statistics = True
auth_type = 'token'
```

**Process Flow**:
1. Connect to Fabric Warehouse
2. Setup external objects (credential, data source, file format)
3. Discover tables in ADLS storage
4. Create schemas if they don't exist
5. Load data using COPY INTO command
6. Optionally validate row counts against source
7. Update statistics on all tables

**Output**:
- Tables created and loaded in Fabric Warehouse
- Loading summary with success/failure counts
- Row count validation results (if enabled)
- Detailed progress logs

**Typical Runtime**: 30-120 minutes (depending on data volume and parallelism)

---

### 03_validate_migration.ipynb

**Purpose**: Comprehensive validation of migration results

**Configuration Parameters**:
```python
source_server = "mysynapse.sql.azuresynapse.net"
source_database = "mydatabase"
target_workspace = "myworkspace"
target_warehouse = "mywarehouse"
generate_report = True
report_path = "/lakehouse/default/Files/validation_report.html"
auth_type = 'token'
```

**Validation Checks**:
1. ✓ Table count comparison
2. ✓ Row count validation for each table
3. ✓ Missing table detection
4. ✓ Extra table detection
5. ✓ Detailed HTML report generation

**Output**:
- Console validation summary
- HTML validation report (if enabled)
- List of matched, mismatched, and missing tables

**Typical Runtime**: 10-30 minutes (depending on number of tables)

---

## Authentication Methods

### Token-Based Authentication (Recommended)

Uses Fabric's managed identity to authenticate:

```python
auth_type = 'token'
```

**Advantages**:
- No credentials needed
- Seamless integration with Fabric
- More secure
- Works in production environments

**Requirements**:
- Running in Fabric notebook environment
- Managed identity configured with appropriate permissions

### Interactive Authentication

Opens browser for manual authentication:

```python
auth_type = 'interactive'
```

**Advantages**:
- Easy for development and testing
- No additional setup required

**Disadvantages**:
- Requires user interaction
- Not suitable for automated workflows

---

## Connection Helper Functions Usage

### Connecting to Azure SQL Database

```python
from migration_helpers import ConnectionHelper

# Token-based authentication
token = ConnectionHelper.get_spark_token("https://database.windows.net/.default")
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase",
    {"auth_type": "token", "token": token}
)

# Interactive authentication
conn = ConnectionHelper.connect_azure_sql(
    "myserver.database.windows.net",
    "mydatabase",
    {"auth_type": "interactive"}
)
```

### Connecting to Fabric Warehouse

```python
from migration_helpers import ConnectionHelper

# Token-based authentication
token = ConnectionHelper.get_spark_token("https://analysis.windows.net/powerbi/api")
conn = ConnectionHelper.connect_fabric_warehouse(
    "myworkspace",
    "mywarehouse",
    {"auth_type": "token", "token": token}
)

# Interactive authentication
conn = ConnectionHelper.connect_fabric_warehouse(
    "myworkspace",
    "mywarehouse",
    {"auth_type": "interactive"}
)
```

### Using Migration Utilities

```python
from migration_helpers import MigrationUtils

# Setup external objects
success = MigrationUtils.setup_external_objects(conn, "storageaccount", "container")

# Get list of tables
tables = MigrationUtils.get_tables_list(conn)

# Validate row counts
result = MigrationUtils.validate_row_count(source_conn, target_conn, "dbo", "customers")
if result['match']:
    print("Row counts match!")
```

### Working with ADLS Storage

```python
from migration_helpers import StorageHelper

# Read Parquet files
df = StorageHelper.read_parquet_with_spark(
    spark,
    "mystorageaccount",
    "migration-staging",
    "dbo/customers"
)

# Write Parquet files
StorageHelper.write_parquet_with_spark(
    df,
    "mystorageaccount",
    "migration-staging",
    "dbo/customers_transformed"
)
```

---

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to database

**Solutions**:
1. Verify ODBC Driver 17 for SQL Server is available
2. Check network connectivity
3. Verify managed identity permissions
4. Try interactive authentication for testing

```python
# Test authentication
token = ConnectionHelper.get_spark_token("https://database.windows.net/.default")
print(f"Token acquired: {token is not None}")
```

### Import Errors

**Problem**: Cannot import migration_helpers

**Solutions**:
1. Verify file path is correct: `/lakehouse/default/Files/notebooks/utils/migration_helpers.py`
2. Check sys.path includes the correct directory
3. Upload the helper file to the correct location in Lakehouse

```python
# Debug import path
import sys
print("Python path:", sys.path)
print("Looking for: /lakehouse/default/Files/notebooks/utils")
```

### External Objects Already Exist

**Problem**: Error creating external objects

**Solutions**:
1. External objects are created with IF NOT EXISTS, so this shouldn't cause issues
2. If needed, manually drop and recreate:

```sql
-- In source database
DROP EXTERNAL TABLE IF EXISTS [schema].[ext_table_migration]
DROP EXTERNAL DATA SOURCE IF EXISTS MigrationStaging
DROP DATABASE SCOPED CREDENTIAL IF EXISTS MigrationCredential
```

### Parquet Read/Write Errors

**Problem**: Cannot read/write Parquet files

**Solutions**:
1. Verify storage account permissions
2. Check container exists
3. Verify ADLS path format

```python
# Verify path
from migration_helpers import StorageHelper
path = StorageHelper.get_adls_path("storageaccount", "container", "schema/table")
print(f"ADLS path: {path}")
```

### Row Count Mismatches

**Problem**: Row counts don't match between source and target

**Solutions**:
1. Check for active transactions in source
2. Verify extraction completed successfully
3. Check for data type conversion issues
4. Review error files in ADLS: `errors/schema/table/`

---

## Performance Optimization

### Parallel Processing

Both extraction and loading notebooks process tables sequentially by default. For better performance:

1. **Extraction**: Large tables (> 1 GB) are automatically partitioned into multiple files
2. **Loading**: COPY INTO handles parallel loading internally

### Batch Size

Adjust batch_size parameter to control memory usage:

```python
batch_size = 50  # Process 50 tables at a time
```

### Resource Allocation

For large migrations, consider:
- Using larger Fabric capacity
- Running during off-peak hours
- Processing tables in batches based on size

---

## Best Practices

1. **Test First**: Run notebooks on a small subset of tables first
2. **Monitor Progress**: Watch cell outputs for errors and warnings
3. **Save Checkpoints**: Run validation after each major step
4. **Review Reports**: Check validation reports before considering migration complete
5. **Document**: Keep notes of any issues and resolutions
6. **Backup**: Ensure source data is backed up before starting migration

---

## Data Type Considerations

Fabric Warehouse has some data type differences from Synapse. See [DATATYPE_MAPPING.md](../DATATYPE_MAPPING.md) for details.

Key considerations:
- `VARCHAR(MAX)` → `VARCHAR(8000)` (Fabric limit)
- `NVARCHAR(MAX)` → `NVARCHAR(4000)` (Fabric limit)
- `DATETIME` → `DATETIME2(3)` (recommended)
- `MONEY` → `DECIMAL(19,4)` (recommended)

The helper functions handle common conversions automatically.

---

## Security Considerations

1. **Never hardcode credentials** in notebooks
2. Use **managed identity** or **token-based auth** in production
3. Store sensitive configuration in **Azure Key Vault**
4. Limit **ADLS access** to migration staging container only
5. Review **permissions** before and after migration

---

## Additional Resources

- [Main Migration Guide](../MIGRATION_GUIDE.md)
- [Permissions Guide](../PERMISSIONS_GUIDE.md)
- [Data Type Mapping](../DATATYPE_MAPPING.md)
- [Best Practices](../BEST_PRACTICES.md)
- [Python Scripts Alternative](../scripts/README.md)

---

## Support

For issues or questions:
1. Review this README and related documentation
2. Check troubleshooting section
3. Verify permissions and connectivity
4. Review notebook cell outputs for specific errors
5. Open an issue in the repository with:
   - Notebook name and cell that failed
   - Error message and stack trace
   - Configuration (without sensitive data)

---

## Comparison: Notebooks vs Scripts

| Feature | PySpark Notebooks | Python Scripts |
|---------|------------------|----------------|
| **Environment** | Fabric Notebooks | Local/VM/Cloud Shell |
| **Interactivity** | High (cell-by-cell) | Low (full script) |
| **Debugging** | Easy (inspect variables) | Standard (logs) |
| **Authentication** | Fabric managed identity | Multiple options |
| **Parallelism** | Sequential (single notebook) | Built-in (ThreadPoolExecutor) |
| **Best For** | Interactive migration, small-medium datasets | Automated migration, large datasets |

**Recommendation**: 
- Use **notebooks** for interactive exploration and small-to-medium migrations
- Use **scripts** for automated, production-grade, and large-scale migrations
