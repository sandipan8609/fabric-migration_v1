# Quick Start: Azure Dedicated Pool to Fabric Warehouse Migration

Get your migration started in **15 minutes** with this quick start guide.

## Prerequisites (5 minutes)

### Required Resources
- ✅ Azure Synapse Dedicated SQL Pool (source)
- ✅ Azure Data Lake Storage Gen2 account
- ✅ Microsoft Fabric workspace with Warehouse
- ✅ Python 3.8+ installed
- ✅ ODBC Driver 17 for SQL Server

### Quick Setup

```bash
# Clone repository (if not already done)
git clone https://github.com/sandipan8609/fabric-migration_v1.git
cd fabric-migration_v1/scripts

# Setup environment
./setup_environment.sh

# Edit configuration
nano .env
```

Edit `.env` with your values:
```bash
SOURCE_SERVER=mysynapse.sql.azuresynapse.net
SOURCE_DATABASE=mydatabase
STORAGE_ACCOUNT=mystorageaccount
STORAGE_CONTAINER=migration-staging
TARGET_WORKSPACE=myworkspace
TARGET_WAREHOUSE=mywarehouse
PARALLEL_JOBS=6
```

## Step 1: Validate Setup (2 minutes)

```bash
# Run pre-migration checks
./pre_migration_checks.sh
```

This validates:
- ✅ Connectivity to source and target
- ✅ Storage account access
- ✅ Permissions
- ✅ ODBC driver

## Step 2: Extract Data (varies by size)

```bash
# Extract data from Azure Dedicated Pool to ADLS
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6
```

**Expected time:**
- Small DB (< 10 GB): 10-30 minutes
- Medium DB (10-100 GB): 30 min - 2 hours
- Large DB (> 100 GB): 2-8 hours

## Step 3: Load Data (varies by size)

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

**Expected time:**
- Small DB (< 10 GB): 5-15 minutes
- Medium DB (10-100 GB): 15 min - 1 hour
- Large DB (> 100 GB): 1-4 hours

## Step 4: Validate Migration (5 minutes)

```bash
# Validate row counts and generate report
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

## Common Issues

### Issue: Connection Failed

**Solution:**
```bash
# Check Azure login
az login

# Verify subscription
az account show
```

### Issue: Permission Denied

**Solution:**
```bash
# Assign storage permissions
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee <your-principal-id> \
    --scope "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<storage-account>"
```

See [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md) for complete permission setup.

### Issue: Datatype Errors

**Solution:**
Check [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md) for datatype compatibility and run:

```python
# Check unsupported datatypes
python3 -c "
from scripts.check_datatypes import check_unsupported_datatypes
check_unsupported_datatypes('mysynapse.sql.azuresynapse.net', 'mydatabase')
"
```

## Performance Tips

### Optimize Parallel Jobs

| Database Size | Extract Jobs | Load Jobs |
|--------------|--------------|-----------|
| < 10 GB | 2-4 | 4-6 |
| 10-100 GB | 4-8 | 6-10 |
| > 100 GB | 8-12 | 10-16 |

### Monitor Progress

During extraction/loading, watch for:
- ✅ Green checkmarks = Success
- ❌ Red X = Failed (check error message)
- Progress counter (e.g., "15/100 tables")

## Next Steps

After successful migration:

1. **Validate Data**
   - Run validation script
   - Spot check critical tables
   - Compare aggregations

2. **Update Statistics**
   ```sql
   -- Run on Fabric Warehouse
   EXEC sp_updatestats;
   ```

3. **Test Queries**
   - Run sample queries
   - Check performance
   - Verify results

4. **Clean Up**
   ```bash
   # Archive or delete staging files (optional)
   az storage blob delete-batch \
       --account-name mystorageaccount \
       --source migration-staging
   ```

## Complete Documentation

For detailed information, see:

- **[Migration Guide](MIGRATION_GUIDE.md)** - Complete migration process
- **[Data Type Mapping](DATATYPE_MAPPING.md)** - Datatype compatibility
- **[Permissions Guide](PERMISSIONS_GUIDE.md)** - Security setup
- **[Scripts README](scripts/README.md)** - Detailed script documentation

## Support

- Check script output for errors
- Review [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) troubleshooting section
- Open an issue in repository

---

**Estimated Total Time:** 30 minutes - 12 hours (depending on database size)

✅ **You're ready to migrate!**
