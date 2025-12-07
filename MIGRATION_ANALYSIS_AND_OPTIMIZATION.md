# Azure Synapse to Fabric Warehouse Migration - Analysis & Optimization Guide

## Executive Summary

This document provides a comprehensive analysis of the current Polybase-based migration approach from Azure Dedicated SQL Pool to Fabric Warehouse, with optimization recommendations for the two-phase approach:
1. **Extraction Phase**: CETAS to dump tables to ADLS
2. **Loading Phase**: COPY INTO to insert data into Fabric Warehouse

---

## Current Architecture Overview

### Phase 1: Data Extraction (CETAS)
```
Azure Synapse Dedicated Pool
    ↓
CETAS (Create External Table As Select)
    ↓
ADLS Gen2 (Parquet Format with Snappy Compression)
    ↓
External Table References
```

### Phase 2: Data Loading (COPY INTO)
```
ADLS Gen2 (Parquet Files)
    ↓
COPY INTO Statement
    ↓
Fabric Warehouse Tables
```

---

## Current Implementation Analysis

### Strengths ✅
1. **Polybase Extraction**: Uses CETAS for parallel, distributed data extraction
2. **Standardized Format**: Uses Parquet with Snappy compression (good balance of size/performance)
3. **Error Handling**: COPY INTO has try-catch blocks to handle failures gracefully
4. **Schema Segregation**: Migration objects isolated in `migration` schema
5. **Managed Identity Support**: Supports authentication without storing credentials
6. **Parameterized Scripts**: Configuration-driven approach

### Current Limitations ⚠️
1. **No Partitioning Strategy**: Large tables not partitioned in ADLS
2. **No Incremental Load Logic**: Full refresh only, no delta handling
3. **Sequential Processing**: Scripts appear to process tables sequentially
4. **Limited Logging**: Minimal monitoring and progress tracking
5. **No Data Validation**: No row count verification post-load
6. **Single Credential Mode**: No separation of concerns for credentials
7. **No Parallel Copy Operations**: COPY INTO likely executes serially
8. **No Statistics Update**: Post-load statistics not being updated
9. **No Table Size Awareness**: Large tables treated same as small tables

---

## Optimization Recommendations

### **Tier 1: High-Impact, Easy Implementation** 🚀

#### 1.1 Add Row Count Validation
**Purpose**: Ensure data integrity post-migration  
**Location**: After COPY INTO completes

```sql
CREATE PROCEDURE migration.sp_validate_data_loads
    @ValidationLogTableName NVARCHAR(256) = 'migration.data_load_validation'
AS
BEGIN
    -- Create validation log table if not exists
    IF OBJECT_ID(@ValidationLogTableName) IS NULL
    BEGIN
        CREATE TABLE migration.data_load_validation (
            validation_id INT IDENTITY(1,1),
            schema_name NVARCHAR(128),
            table_name NVARCHAR(128),
            source_row_count BIGINT,
            target_row_count BIGINT,
            match_status VARCHAR(10),
            validation_timestamp DATETIME2 DEFAULT GETDATE()
        )
    END
    
    -- Validate each table
    INSERT INTO migration.data_load_validation 
    (schema_name, table_name, source_row_count, target_row_count, match_status)
    SELECT 
        sc.name,
        tbl.name,
        (SELECT CAST(value AS BIGINT) FROM sys.dm_pdw_nodes_db_partition_stats 
         WHERE object_id = tbl.object_id AND index_id <= 1),
        (SELECT COUNT(*) FROM ??? ), -- Target table count
        CASE WHEN source_count = target_count THEN 'PASS' ELSE 'FAIL' END
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration'
END
GO
```

#### 1.2 Update Statistics Post-Load
**Purpose**: Ensure optimal query execution in Fabric  
**Location**: After COPY INTO and validation

```sql
CREATE PROCEDURE migration.sp_update_table_statistics
AS
BEGIN
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(128)
    DECLARE table_cursor CURSOR FOR
    SELECT sc.name, tbl.name
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration'
    
    OPEN table_cursor
    FETCH NEXT FROM table_cursor INTO @schema, @table
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @sql NVARCHAR(MAX) = 'UPDATE STATISTICS [' + @schema + '].[' + @table + ']'
        EXEC sp_executesql @sql
        PRINT 'Updated stats for [' + @schema + '].[' + @table + ']'
        
        FETCH NEXT FROM table_cursor INTO @schema, @table
    END
    
    CLOSE table_cursor
    DEALLOCATE table_cursor
END
GO
```

#### 1.3 Add Comprehensive Logging
**Purpose**: Track migration progress and troubleshoot issues

```sql
CREATE TABLE migration.migration_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    phase VARCHAR(50),
    table_name NVARCHAR(256),
    operation VARCHAR(100),
    status VARCHAR(20),
    rows_processed BIGINT,
    duration_seconds INT,
    error_message NVARCHAR(MAX),
    log_timestamp DATETIME2 DEFAULT GETDATE()
)

CREATE PROCEDURE migration.sp_log_migration_event
    @Phase VARCHAR(50),
    @TableName NVARCHAR(256),
    @Operation VARCHAR(100),
    @Status VARCHAR(20),
    @RowsProcessed BIGINT = NULL,
    @DurationSeconds INT = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    INSERT INTO migration.migration_log 
    VALUES (@Phase, @TableName, @Operation, @Status, @RowsProcessed, @DurationSeconds, @ErrorMessage)
END
GO
```

---

### **Tier 2: Medium-Impact, Moderate Effort** ⚡

#### 2.1 Implement Table Size-Based Partitioning
**Purpose**: Optimize extraction and loading for very large tables  
**Implementation**:

```sql
CREATE PROCEDURE migration.sp_cetas_extract_with_partitioning
    @adls_gen2_location VARCHAR(1024),
    @storage_access_token VARCHAR(1024),
    @partition_size_mb INT = 1000 -- Files of ~1GB each
AS
BEGIN
    -- For large tables (>1GB), partition by hash of key columns
    -- For small tables, single file extraction
    
    DECLARE @table_id INT, @table_name NVARCHAR(256), @schema_name NVARCHAR(128)
    DECLARE @table_size_mb BIGINT, @partition_count INT
    
    DECLARE table_cursor CURSOR FOR
    SELECT 
        t.object_id,
        s.name,
        t.name,
        (SUM(ps.reserved_page_count) * 8 / 1024) AS size_mb
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.partitions p ON t.object_id = p.object_id
    LEFT JOIN sys.dm_db_partition_stats ps ON t.object_id = ps.object_id
    WHERE s.name != 'migration' AND t.is_external = 0
    GROUP BY t.object_id, s.name, t.name
    ORDER BY size_mb DESC
    
    OPEN table_cursor
    FETCH NEXT FROM table_cursor INTO @table_id, @schema_name, @table_name, @table_size_mb
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @partition_count = CASE 
            WHEN @table_size_mb > 10000 THEN CEILING(@table_size_mb / @partition_size_mb)
            ELSE 1
        END
        
        -- Generate partitioned CETAS for large tables
        PRINT 'Table: ' + @schema_name + '.' + @table_name + 
              ' Size: ' + CAST(@table_size_mb AS VARCHAR) + 'MB' + 
              ' Partitions: ' + CAST(@partition_count AS VARCHAR)
        
        -- Execute partitioned extraction
        
        FETCH NEXT FROM table_cursor INTO @table_id, @schema_name, @table_name, @table_size_mb
    END
    
    CLOSE table_cursor
    DEALLOCATE table_cursor
END
GO
```

#### 2.2 Implement Parallel COPY INTO Operations
**Purpose**: Load multiple tables concurrently  
**Implementation Approach**: PowerShell multi-threading

```powershell
# Add to Gen2-Fabric DW Migration.ps1

function Start-ParallelCopyIntoJobs {
    param(
        [array]$DataLoadStatements,
        [int]$MaxParallelJobs = 4,
        [string]$TargetConnectionString,
        [string]$AccessToken
    )
    
    $jobs = @()
    $count = 0
    
    foreach ($statement in $DataLoadStatements) {
        # Wait if max parallel jobs reached
        while ((Get-Job -State Running | Measure-Object).Count -ge $MaxParallelJobs) {
            Start-Sleep -Seconds 5
        }
        
        # Start job
        $job = Start-Job -ScriptBlock {
            param($sql, $connStr, $token)
            # Execute COPY INTO
            $connection = New-Object System.Data.SqlClient.SqlConnection
            $connection.ConnectionString = $connStr
            $connection.AccessToken = $token
            $connection.Open()
            $command = $connection.CreateCommand()
            $command.CommandText = $sql
            $command.CommandTimeout = 0 # No timeout
            $command.ExecuteNonQuery()
            $connection.Close()
        } -ArgumentList $statement, $TargetConnectionString, $AccessToken
        
        $jobs += $job
        $count++
        Write-Host "Started job $count - Loading..."
    }
    
    # Wait for all jobs
    Get-Job | Wait-Job
    
    # Collect results
    foreach ($job in $jobs) {
        $result = Receive-Job -Job $job -ErrorAction Stop
        Write-Host "Job completed: $($job.Name)"
        Remove-Job -Job $job
    }
}
```

#### 2.3 Add Data Type Migration Warnings
**Purpose**: Flag potential data type compatibility issues

```sql
CREATE PROCEDURE migration.sp_check_unsupported_datatypes
AS
BEGIN
    SELECT 
        sc.name AS schema_name,
        tbl.name AS table_name,
        col.name AS column_name,
        tp.name AS data_type,
        col.max_length,
        col.precision,
        col.scale,
        'WARNING: Verify compatibility in Fabric' AS recommendation
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    INNER JOIN sys.columns col ON tbl.object_id = col.object_id
    INNER JOIN sys.types tp ON col.system_type_id = tp.system_type_id
    WHERE sc.name != 'migration'
    AND tp.name IN ('geometry', 'geography', 'hierarchyid', 'xml', 'image', 'text', 'ntext')
    ORDER BY sc.name, tbl.name, col.name
END
GO
```

---

### **Tier 3: Advanced Optimizations** 🎯

#### 3.1 Implement Incremental Load Strategy
**Purpose**: Support delta migrations for large tables

```sql
CREATE TABLE migration.table_load_history (
    load_id BIGINT IDENTITY(1,1),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(256),
    load_type VARCHAR(20), -- 'FULL' or 'DELTA'
    source_row_count BIGINT,
    loaded_row_count BIGINT,
    last_modified_column NVARCHAR(256),
    load_start_time DATETIME2,
    load_end_time DATETIME2,
    load_status VARCHAR(20)
)

CREATE PROCEDURE migration.sp_generate_incremental_cetas
    @adls_gen2_location VARCHAR(1024),
    @storage_access_token VARCHAR(1024),
    @load_type VARCHAR(20) = 'FULL' -- or 'DELTA'
AS
BEGIN
    IF @load_type = 'DELTA'
    BEGIN
        -- Generate CETAS for only modified records
        -- Requires tracking last successful load timestamp
        SELECT 
            sc.name SchName,
            tbl.name objName,
            'IF (object_id('''+ sc.name + '.delta_' + tbl.name + ''',''U'') IS NOT NULL) DROP EXTERNAL TABLE ['+ sc.name + '].[delta_' + tbl.name +'];' AS DropStatement,
            'CREATE EXTERNAL TABLE [' + sc.name + '].[delta_' + tbl.name + 
                '] WITH (LOCATION = ''/'+sc.name+'/'+tbl.name+'/delta/''' + ',' + 
                ' DATA_SOURCE = fabric_data_migration_ext_data_source,' +
                ' FILE_FORMAT = fabric_data_migration_ext_file_format)' +
                ' AS SELECT * FROM [' + sc.name + '].[' + tbl.name + ']
                   WHERE ModifiedDate > (SELECT MAX(load_end_time) FROM migration.table_load_history 
                                        WHERE table_name = ''' + tbl.name + ''');' AS data_extract_statement   
        FROM sys.tables tbl
        INNER JOIN sys.schemas sc ON tbl.schema_id=sc.schema_id
        WHERE sc.name !='migration' AND tbl.is_external = 'false'
    END
    ELSE
    BEGIN
        -- Full load (existing logic)
        EXEC migration.sp_cetas_extract_script @adls_gen2_location, @storage_access_token
    END
END
GO
```

#### 3.2 Implement Cost Optimization (File Format Selection)
**Purpose**: Reduce storage and scan costs

```sql
-- Add cost analysis before extraction
CREATE PROCEDURE migration.sp_analyze_compression_options
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(256)
AS
BEGIN
    -- Analyze table characteristics to recommend optimal compression
    DECLARE @row_count BIGINT, @avg_row_size INT
    
    SELECT 
        @row_count = COUNT(*),
        @avg_row_size = AVG(DATALENGTH(CAST(* AS VARCHAR(MAX))))
    FROM ???
    
    -- Recommend compression based on characteristics
    IF @row_count > 100000000 AND @avg_row_size > 500
    BEGIN
        PRINT 'Recommendation: Use DELTA format for this large table'
    END
    ELSE
    BEGIN
        PRINT 'Recommendation: Continue with Parquet Snappy'
    END
END
GO
```

#### 3.3 Add Fabric-Specific Performance Tuning
**Purpose**: Optimize for Fabric Warehouse characteristics

```sql
-- Execute after loading to Fabric
CREATE PROCEDURE migration.sp_optimize_fabric_tables
    @TargetDatabase NVARCHAR(256)
AS
BEGIN
    -- For Fabric, index strategy is different than Synapse
    -- Focus on clustered columnstore indexes if not exists
    
    DECLARE @sql NVARCHAR(MAX)
    
    DECLARE table_cursor CURSOR FOR
    SELECT sc.name, t.name, t.object_id
    FROM sys.tables t
    INNER JOIN sys.schemas sc ON t.schema_id = sc.schema_id
    WHERE sc.name != 'migration'
    
    OPEN table_cursor
    
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(256), @object_id INT
    FETCH NEXT FROM table_cursor INTO @schema, @table, @object_id
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Check if clustered columnstore index exists
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes 
            WHERE object_id = @object_id AND type = 5 -- Clustered columnstore
        )
        BEGIN
            -- Drop existing clustered indexes if any
            DECLARE @index_cursor CURSOR
            SET @index_cursor = CURSOR FOR
            SELECT name FROM sys.indexes 
            WHERE object_id = @object_id AND type IN (1) -- Clustered
            
            OPEN @index_cursor
            DECLARE @index_name NVARCHAR(256)
            FETCH NEXT FROM @index_cursor INTO @index_name
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql = 'DROP INDEX [' + @index_name + '] ON [' + @schema + '].[' + @table + ']'
                EXEC sp_executesql @sql
                FETCH NEXT FROM @index_cursor INTO @index_name
            END
            
            CLOSE @index_cursor
            DEALLOCATE @index_cursor
            
            -- Create clustered columnstore
            SET @sql = 'CREATE CLUSTERED COLUMNSTORE INDEX CCI_' + @table + 
                       ' ON [' + @schema + '].[' + @table + ']'
            EXEC sp_executesql @sql
            PRINT 'Created CCI for [' + @schema + '].[' + @table + ']'
        END
        
        FETCH NEXT FROM table_cursor INTO @schema, @table, @object_id
    END
    
    CLOSE table_cursor
    DEALLOCATE table_cursor
END
GO
```

---

## Implementation Roadmap

### Phase 1: Foundation (Week 1-2)
- [ ] Implement logging framework (1.3)
- [ ] Add row count validation (1.1)
- [ ] Update statistics procedure (1.2)
- [ ] Test with subset of tables

### Phase 2: Performance (Week 3-4)
- [ ] Implement parallel COPY INTO (2.2)
- [ ] Add table partitioning for large tables (2.1)
- [ ] Data type compatibility checks (2.3)
- [ ] Performance baseline testing

### Phase 3: Advanced (Week 5-6)
- [ ] Implement incremental load strategy (3.1)
- [ ] Cost optimization analysis (3.2)
- [ ] Fabric-specific tuning (3.3)
- [ ] End-to-end testing

---

## Performance Benchmarks & Targets

| Metric | Current | Target | Optimization |
|--------|---------|--------|---------------|
| Extraction Speed | ~100-500 MB/s | 500-1000 MB/s | Parallel CETAS with partitioning |
| Load Speed | ~100-300 MB/s | 300-800 MB/s | Parallel COPY INTO |
| Storage Cost | Baseline | -30-40% | Format optimization |
| Migration Duration | Sequential | -50% | Parallelization |
| Data Validation | Manual | Automated | Logging & validation procedures |
| Error Recovery | Manual | Automatic | Enhanced error handling & retry logic |

---

## Best Practices Checklist

### Pre-Migration
- [ ] Inventory all tables and their sizes
- [ ] Identify tables with unsupported data types
- [ ] Plan partitioning strategy for tables >1GB
- [ ] Document baseline row counts
- [ ] Backup source database

### During Migration
- [ ] Monitor parallel job status
- [ ] Check storage account throttling
- [ ] Validate no schema drift
- [ ] Track migration duration by table
- [ ] Monitor target database space utilization

### Post-Migration
- [ ] Validate row counts match
- [ ] Update table statistics
- [ ] Create clustered columnstore indexes
- [ ] Run query plans on sample queries
- [ ] Decommission ADLS interim storage
- [ ] Document lessons learned

---

## Troubleshooting Guide

### Issue: COPY INTO Fails with Authentication Error
**Solution**: Verify storage account key hasn't expired, check managed identity permissions
```sql
-- Test connectivity
SELECT * FROM (SELECT 'test') t
OPENROWSET(BULK 'path/file.parquet', 
    DATA_SOURCE = 'fabric_data_migration_ext_data_source',
    FORMAT = 'PARQUET')
```

### Issue: Extraction Timeout
**Solution**: Implement table-level partitioning or skip large intermediate tables
```sql
-- Check estimated data size
SELECT 
    OBJECT_NAME(ps.object_id) AS table_name,
    SUM(ps.reserved_page_count) * 8 / 1024 AS size_mb
FROM sys.dm_db_partition_stats ps
GROUP BY ps.object_id
ORDER BY size_mb DESC
```

### Issue: Target Database Throttling
**Solution**: Implement exponential backoff in COPY INTO retry logic
```sql
-- Add retry with delay
DECLARE @retry INT = 0
WHILE @retry < 3
BEGIN
    BEGIN TRY
        COPY INTO [schema].[table] FROM '...' ...
        SET @retry = 3 -- Success, exit loop
    END TRY
    BEGIN CATCH
        SET @retry = @retry + 1
        IF @retry < 3
            WAITFOR DELAY '00:00:10' -- 10 seconds
        ELSE
            THROW
    END CATCH
END
```

---

## Additional Resources

- [Fabric Warehouse COPY INTO Documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-copy-into)
- [Polybase External Tables Guide](https://learn.microsoft.com/en-us/sql/relational-databases/polybase/polybase-guide)
- [Azure Synapse to Fabric Migration Guide](https://aka.ms/fabric-migrate-synapse-dw)
- [Fabric Performance Tuning](https://learn.microsoft.com/en-us/fabric/data-warehouse/tutorial-query-performance)

---

## Questions & Clarifications

**Q: Should we use Managed Identity or Storage Keys?**  
A: Managed Identity is more secure, but requires proper RBAC setup. Storage Keys are simpler but require rotation policies.

**Q: What's the optimal ADLS folder structure?**  
A: `/schema/table_name/` or `/schema/table_name/year=2024/month=12/` for better data governance

**Q: Do we need to keep extracted Parquet files post-migration?**  
A: Recommended for 30-90 days as rollback safety, then archive to cold storage

---

**Document Version**: 1.0  
**Last Updated**: December 2024  
**Author**: Analysis & Optimization Team
