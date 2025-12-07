# Implementation Guide: Optimized Synapse to Fabric Migration

## Quick Start (30 Minutes)

### Phase 1: Setup & Assessment (10 min)

```powershell
# 1. Deploy optimization framework to source Synapse
Invoke-Sqlcmd -ServerInstance "mysynapse.sql.azuresynapse.net" `
    -Database "mydb" `
    -InputFile ".\optimization_framework.sql" `
    -AccessToken $token

# 2. Run compatibility check
Invoke-Sqlcmd -ServerInstance "mysynapse.sql.azuresynapse.net" `
    -Database "mydb" `
    -Query "EXEC migration.sp_check_unsupported_datatypes" `
    -AccessToken $token

# 3. Analyze table sizes and structure
Invoke-Sqlcmd -ServerInstance "mysynapse.sql.azuresynapse.net" `
    -Database "mydb" `
    -Query "EXEC migration.sp_analyze_table_sizes_and_partitions" `
    -AccessToken $token
```

### Phase 2: Extract Data (10-60 min depending on size)

```powershell
# Run existing extraction script (unchanged)
.\Gen2-Fabric\ DW\ Migration.ps1 `
    -Server "mysynapse.sql.azuresynapse.net" `
    -Database "mydb" `
    -adls_gen2_location "abfss://container@account.dfs.core.windows.net/" `
    -storage_access_token "your_access_key" `
    -extractDataFromSource $true
```

### Phase 3: Load Data with Optimization (5-60 min depending on size)

```powershell
# Run optimized loading script with parallel operations
.\Optimized-Fabric-Migration.ps1 `
    -ServerInstance "mysynapse.sql.azuresynapse.net" `
    -Database "mydb" `
    -StorageAccessToken "your_access_key" `
    -AdlsLocation "abfss://container@account.dfs.core.windows.net/" `
    -TargetServerName "myworkspace.fabric.microsoft.com" `
    -TargetDatabase "fabric_db" `
    -MaxParallelLoads 4 `
    -EnableValidation $true `
    -EnableStatisticsUpdate $true
```

### Phase 4: Validation & Reporting (5 min)

```powershell
# Get comprehensive migration report
Invoke-Sqlcmd -ServerInstance "myworkspace.fabric.microsoft.com" `
    -Database "fabric_db" `
    -Query "EXEC migration.sp_get_migration_progress_report" `
    -AccessToken $fabric_token
```

---

## Configuration Examples

### Example 1: Small Database (< 10GB)

```powershell
# Single-phase migration with validation
$params = @{
    ServerInstance = "mysynapse.sql.azuresynapse.net"
    Database = "small_db"
    StorageAccessToken = "key"
    AdlsLocation = "abfss://container@account.dfs.core.windows.net/"
    TargetServerName = "workspace.fabric.microsoft.com"
    TargetDatabase = "fabric_db"
    MaxParallelLoads = 2  # Conservative for small database
    EnableValidation = $true
    EnableStatisticsUpdate = $true
}

.\Optimized-Fabric-Migration.ps1 @params
```

### Example 2: Large Database (100GB - 1TB)

```powershell
# Multi-phase migration with aggressive parallelization
$params = @{
    ServerInstance = "mysynapse.sql.azuresynapse.net"
    Database = "large_db"
    StorageAccessToken = "key"
    AdlsLocation = "abfss://container@account.dfs.core.windows.net/"
    TargetServerName = "workspace.fabric.microsoft.com"
    TargetDatabase = "fabric_db"
    MaxParallelLoads = 8  # Aggressive parallelization
    EnableValidation = $true
    EnableStatisticsUpdate = $true
    LogPath = ".\logs\migration_large_$(Get-Date -Format 'yyyyMMdd').log"
}

.\Optimized-Fabric-Migration.ps1 @params

# After initial load, run incremental load for delta tables
# (Add incremental logic as needed)
```

### Example 3: Premium Setup with Full Monitoring

```powershell
# Enterprise-grade migration with enhanced monitoring
$ErrorActionPreference = 'Continue'

# Pre-migration assessment
Write-Host "Starting pre-migration assessment..." -ForegroundColor Green

$sourceToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net/).Token
$assessment = Invoke-Sqlcmd `
    -ServerInstance "mysynapse.sql.azuresynapse.net" `
    -Database "enterprise_db" `
    -Query @"
        SELECT 
            COUNT(*) AS table_count,
            SUM(size_mb) AS total_size_mb,
            SUM(CASE WHEN partitioning_recommended = 1 THEN 1 ELSE 0 END) AS needs_partitioning
        FROM migration.table_size_analysis
"@ `
    -AccessToken $sourceToken

Write-Host "Assessment Results:" -ForegroundColor Cyan
Write-Host "  Tables: $($assessment[0]['table_count'])"
Write-Host "  Total Size: $($assessment[0]['total_size_mb']) MB"
Write-Host "  Partitioning Required: $($assessment[0]['needs_partitioning']) tables"

# Run migration
$params = @{
    ServerInstance = "mysynapse.sql.azuresynapse.net"
    Database = "enterprise_db"
    StorageAccessToken = "key"
    AdlsLocation = "abfss://container@account.dfs.core.windows.net/"
    TargetServerName = "workspace.fabric.microsoft.com"
    TargetDatabase = "fabric_db"
    MaxParallelLoads = 8
    EnableValidation = $true
    EnableStatisticsUpdate = $true
}

.\Optimized-Fabric-Migration.ps1 @params

# Post-migration reporting
Write-Host "Migration completed. Generating reports..." -ForegroundColor Green

$fabricToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net/).Token
$report = Invoke-Sqlcmd `
    -ServerInstance "workspace.fabric.microsoft.com" `
    -Database "fabric_db" `
    -Query "EXEC migration.sp_get_migration_progress_report" `
    -AccessToken $fabricToken

Write-Host $report
```

---

## Optimization Strategies by Database Size

### Micro (< 1 GB)
- Single table load sequentially
- No partitioning needed
- Validation via simple count check
- Time: 5-15 minutes

### Small (1-10 GB)
- **Parallelism**: 2-3 concurrent loads
- **Validation**: Row count check per table
- **Stats**: Update once post-load
- Time: 15-45 minutes

### Medium (10-100 GB)
- **Parallelism**: 4-6 concurrent loads
- **Strategy**: Load large tables first
- **Partitioning**: Consider for tables > 5GB
- **Validation**: Comprehensive with sampling
- **Stats**: Update per schema after load
- Time: 1-4 hours

### Large (100 GB - 1 TB)
- **Parallelism**: 8-10 concurrent loads
- **Strategy**: 
  - Load large tables with partitioning
  - Load medium/small tables in parallel
  - Use incremental loads for fact tables
- **Validation**: Full validation with data quality checks
- **Stats**: Update per table immediately after load
- Time: 4-24 hours

### Very Large (> 1 TB)
- **Multi-batch approach**: Load tables in batches
- **Parallelism**: 10-16 concurrent (monitor throttling)
- **Partitioning**: Required for all tables > 10GB
- **Incremental**: Use delta for large tables
- **Monitoring**: Real-time progress tracking
- **Validation**: Continuous validation
- Time: 24+ hours (staged approach recommended)

---

## Performance Tuning Checklist

### Pre-Load
- [ ] Verify ADLS path structure: `/schema/table/`
- [ ] Check storage account network rules
- [ ] Verify managed identity has Storage Blob Data Reader role
- [ ] Disable auto-pause on source Synapse during extraction
- [ ] Disable auto-pause on target Fabric during loading
- [ ] Check for running queries on target (can block COPY INTO)

### During Load
- [ ] Monitor CPU and memory on Fabric warehouse
- [ ] Check storage account throttling (HTTP 429 errors)
- [ ] Track number of active parallel jobs
- [ ] Monitor query execution time per table
- [ ] Verify no network timeout errors

### Post-Load
- [ ] [ ] Validate all tables loaded successfully
- [ ] [ ] Verify row counts match source
- [ ] [ ] Update table statistics
- [ ] [ ] Create/rebuild clustered columnstore indexes
- [ ] [ ] Run sample queries to verify data integrity
- [ ] [ ] Check for NULL values in critical columns

---

## Troubleshooting Common Issues

### Issue 1: "Storage account not found" Error

**Symptom**: COPY INTO fails with "Could not find data source 'fabric_data_migration_ext_data_source'"

**Solution**:
```sql
-- Verify external data source exists
SELECT * FROM sys.external_data_sources

-- Recreate if needed
CREATE EXTERNAL DATA SOURCE fabric_data_migration_ext_data_source 
WITH (
    TYPE = hadoop,
    LOCATION = 'abfss://container@account.dfs.core.windows.net/',
    CREDENTIAL = fabric_migration_credential
)
```

### Issue 2: "Authentication failed"

**Symptom**: COPY INTO fails with "Login failed" or "Access denied"

**Solution**:
```powershell
# Verify storage account key is still valid
$storageAccount = Get-AzStorageAccount -ResourceGroupName $rg -Name $accountName
$keys = Get-AzStorageAccountKey -ResourceGroupName $rg -Name $accountName
$key1 = $keys[0].Value

# Update connection if key was rotated
# Regenerate COPY INTO statements with new key
```

### Issue 3: Throttling (HTTP 429 errors)

**Symptom**: Multiple COPY INTO operations fail with "Service Unavailable"

**Solution**:
```powershell
# Reduce parallelism
$MaxParallelLoads = 4  # Reduce from 8

# Add backoff in retry logic
$retry_delay = 30  # Increase from 5 seconds

# Alternative: Stagger loads
Start-Sleep -Seconds 30  # Between batch submissions
```

### Issue 4: Timeout Errors

**Symptom**: COPY INTO times out for large tables

**Solution**:
```sql
-- Increase command timeout (no server-side limit for COPY INTO)
-- Ensure source database has sufficient resources
-- Check for blocking queries:
SELECT * FROM sys.dm_exec_requests WHERE status = 'running'

-- Partition large tables before extraction
-- Load partitions sequentially if needed
```

### Issue 5: Data Type Mismatch

**Symptom**: COPY INTO fails with data type conversion error

**Solution**:
```sql
-- Identify the column
SELECT * FROM migration.unsupported_datatype_log

-- Convert in source before extraction
-- OR create view with CAST
CREATE VIEW migration.vw_table_converted AS
SELECT 
    CAST(geometry_col AS VARBINARY(MAX)) AS geometry_col_binary,
    -- other columns
FROM source_table

-- Extract from view instead
```

---

## Performance Benchmarks

### Expected Throughput by Table Size

| Table Size | Extraction Speed | Load Speed | Total Time |
|-----------|-----------------|-----------|-----------|
| 100 MB | 500 MB/s | 300 MB/s | 1 min |
| 1 GB | 600 MB/s | 350 MB/s | 6 min |
| 10 GB | 700 MB/s | 400 MB/s | 50 min |
| 100 GB | 800 MB/s | 450 MB/s | 6 hours |
| 1 TB | 900 MB/s | 500 MB/s | 30+ hours |

**Note**: Actual performance depends on:
- Network bandwidth
- Storage account SKU
- Fabric warehouse SKU
- Parallelism degree
- Compression efficiency

---

## Monitoring Dashboard (T-SQL)

```sql
-- Real-time migration progress
SELECT 
    phase,
    COUNT(DISTINCT table_name) AS tables_processed,
    SUM(rows_processed) AS total_rows,
    AVG(duration_seconds) AS avg_duration_sec,
    MAX(log_timestamp) AS last_activity
FROM migration.migration_log
WHERE log_timestamp >= DATEADD(HOUR, -2, GETDATE())
GROUP BY phase

-- Per-table performance
SELECT TOP 20
    table_name,
    SUM(rows_processed) AS total_rows,
    SUM(file_size_mb) AS total_size_mb,
    AVG(duration_seconds) AS avg_load_time,
    COUNT(*) AS load_attempts,
    CASE WHEN COUNT(*) > 1 THEN 'RETRY_REQUIRED' ELSE 'OK' END AS status
FROM migration.migration_log
WHERE phase = 'LOADING'
GROUP BY table_name
ORDER BY total_rows DESC

-- Failure analysis
SELECT 
    table_name,
    error_message,
    COUNT(*) AS frequency,
    MAX(log_timestamp) AS last_occurrence
FROM migration.migration_log
WHERE status = 'FAILED'
GROUP BY table_name, error_message
ORDER BY frequency DESC
```

---

## Estimated Costs

### Azure Storage (ADLS Gen2)
- **Extraction cost**: ~$0.05 per GB (read operations)
- **Storage cost**: ~$0.018 per GB/month (standard tier)
- **Expected duration**: 30-90 days = $0.54-1.62 per GB
- **Recommendation**: Archive or delete after 90 days

### Fabric Warehouse
- **No extraction cost** (internal ADLS read)
- **DWU cost**: ~$5.75 per DWU per hour
- **Typical load time**: 1-2 hours = $11-23 for medium migration

### Synapse Dedicated Pool (if using)
- **Extraction time**: 1-4 hours depending on size
- **Cost**: ~$10-40 for medium migration

**Total estimated cost for 100GB migration**: $25-65 (plus storage)

---

## Success Criteria

- [ ] All tables extracted to ADLS in Parquet format
- [ ] Row count validation: 100% match between source and target
- [ ] No data type conversion errors
- [ ] Query performance: Sample queries execute in < 5 seconds
- [ ] Statistics updated on all tables
- [ ] Clustered columnstore indexes created where appropriate
- [ ] No orphaned external tables in source
- [ ] ADLS cleanup plan documented

---

## Next Steps

1. **Review** the analysis document: `MIGRATION_ANALYSIS_AND_OPTIMIZATION.md`
2. **Test** with 1-2 small tables first
3. **Validate** extraction results
4. **Run** full migration with selected configuration
5. **Monitor** using provided dashboards
6. **Optimize** parallelism based on actual performance
7. **Document** lessons learned

---

**Document Version**: 1.0  
**Last Updated**: December 2024
