# Migration Scripts

This directory contains Python and Bash scripts for migrating data from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse.

## Scripts Overview

### Python Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| **extract_data.py** | Extract data from Azure Synapse to ADLS using CETAS | Required |
| **load_data.py** | Load data from ADLS to Fabric Warehouse using COPY INTO | Required |
| **validate_migration.py** | Validate row counts and data integrity | Recommended |

### Bash Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| **setup_environment.sh** | Setup Python dependencies and environment | Run first |
| **pre_migration_checks.sh** | Validate connectivity and permissions | Run before migration |

### Configuration Files

| File | Purpose |
|------|---------|
| **requirements.txt** | Python package dependencies |
| **.env** | Environment configuration (created by setup script) |

---

## Quick Start

### 1. Setup Environment

```bash
# Make scripts executable
chmod +x *.sh

# Run setup
./setup_environment.sh

# Edit configuration
nano .env
```

### 2. Pre-Migration Checks

```bash
# Validate connectivity and permissions
./pre_migration_checks.sh
```

### 3. Extract Data

```bash
# Extract data from Azure Synapse to ADLS
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6
```

### 4. Load Data

```bash
# Load data from ADLS to Fabric Warehouse
python3 load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase
```

### 5. Validate Migration

```bash
# Validate row counts and generate report
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

---

## Script Details

### extract_data.py

Extracts data from Azure Synapse Dedicated SQL Pool to Azure Data Lake Storage Gen2.

**Features:**
- Parallel extraction for faster performance
- Automatic table discovery
- External table creation (CETAS)
- Parquet file format with compression
- Progress tracking and colored output
- Error handling and logging

**Options:**
```
--server             Synapse server name (required)
--database           Database name (required)
--storage-account    Storage account name (required)
--container          Container name (required)
--parallel-jobs      Number of parallel jobs (default: 4)
--enable-partitioning Enable partitioning for large tables (default: True)
```

**Example:**
```bash
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6
```

---

### load_data.py

Loads data from Azure Data Lake Storage Gen2 to Microsoft Fabric Warehouse.

**Features:**
- Parallel loading for faster performance
- Automatic schema and table creation
- COPY INTO with retry logic
- Row count validation against source
- Statistics updates post-load
- Progress tracking and colored output
- Error handling and logging

**Options:**
```
--workspace          Fabric workspace name (required)
--warehouse          Fabric warehouse name (required)
--storage-account    Storage account name (required)
--container          Container name (required)
--parallel-jobs      Number of parallel jobs (default: 4)
--validate-rows      Validate row counts against source
--source-server      Source server for validation
--source-database    Source database for validation
--update-stats       Update statistics after loading (default: True)
```

**Example:**
```bash
python3 load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase
```

---

### validate_migration.py

Validates migration by comparing source and target databases.

**Features:**
- Row count comparison
- Missing/extra table detection
- Data type validation (optional)
- Detailed validation report
- Colored output for easy reading

**Options:**
```
--source-server      Source Synapse server (required)
--source-database    Source database name (required)
--target-workspace   Target Fabric workspace (required)
--target-warehouse   Target Fabric warehouse (required)
--generate-report    Generate detailed validation report
```

**Example:**
```bash
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

---

## Authentication

Scripts support multiple authentication methods (in order of preference):

1. **Service Principal** (if environment variables set):
   - `AZURE_TENANT_ID`
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`

2. **Default Azure Credential** (automatic):
   - Managed Identity (if running on Azure VM/container)
   - Azure CLI login (`az login`)
   - Environment variables
   - Interactive browser login

3. **Interactive Authentication** (fallback):
   - Opens browser for manual login

**Recommended:** Use service principal or managed identity for production, Azure CLI for development.

---

## Environment Variables

Create a `.env` file (auto-created by setup script):

```bash
# Azure Credentials (optional)
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# Source Configuration
SOURCE_SERVER=mysynapse.sql.azuresynapse.net
SOURCE_DATABASE=mydatabase

# Storage Configuration
STORAGE_ACCOUNT=mystorageaccount
STORAGE_CONTAINER=migration-staging

# Target Configuration
TARGET_WORKSPACE=myworkspace
TARGET_WAREHOUSE=mywarehouse

# Migration Settings
PARALLEL_JOBS=6
VALIDATE_ROWS=true
```

⚠️ **Important:** Add `.env` to `.gitignore` to prevent committing credentials!

---

## Performance Tuning

### Parallel Jobs

Adjust based on database size and resources:

| Database Size | Recommended Parallel Jobs |
|--------------|--------------------------|
| < 10 GB | 2-4 jobs |
| 10-100 GB | 4-8 jobs |
| 100 GB - 1 TB | 8-12 jobs |
| > 1 TB | 12-16 jobs |

### Extraction Performance

- Small tables (< 100 MB): No partitioning needed
- Large tables (> 1 GB): Automatic partitioning enabled
- Very large tables (> 100 GB): Consider manual partitioning

### Loading Performance

- Parallel COPY INTO operations
- Automatic retry with exponential backoff
- Statistics updates after loading

---

## Error Handling

All scripts include:
- ✅ Automatic retry logic
- ✅ Detailed error messages
- ✅ Progress tracking
- ✅ Colored output for easy identification
- ✅ Graceful failure handling

**Exit codes:**
- `0` - Success
- `1` - Failure (check output for details)

---

## Troubleshooting

### Connection Issues

```bash
# Test connectivity
./pre_migration_checks.sh

# Check Azure login
az account show

# Test ODBC driver
odbcinst -q -d
```

### Permission Issues

Refer to [PERMISSIONS_GUIDE.md](../PERMISSIONS_GUIDE.md) for detailed permission setup.

### Data Type Issues

Refer to [DATATYPE_MAPPING.md](../DATATYPE_MAPPING.md) for data type compatibility.

---

## Additional Resources

- [Main Migration Guide](../MIGRATION_GUIDE.md)
- [Data Type Mapping Guide](../DATATYPE_MAPPING.md)
- [Permissions Guide](../PERMISSIONS_GUIDE.md)
- [Best Practices](../BEST_PRACTICES.md)

---

## Support

For issues:
1. Check script output for error messages
2. Review documentation guides
3. Run pre-migration checks
4. Verify permissions and connectivity
5. Open an issue in the repository
