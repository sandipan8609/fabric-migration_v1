-- ============================================================================
-- OPTIMIZATION SCRIPTS FOR SYNAPSE TO FABRIC MIGRATION
-- Purpose: Enhanced logging, validation, and performance tracking
-- ============================================================================

-- ============================================================================
-- 1. MIGRATION LOGGING FRAMEWORK
-- ============================================================================

IF OBJECT_ID('migration.migration_log', 'U') IS NOT NULL
    DROP TABLE migration.migration_log
GO

CREATE TABLE migration.migration_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    phase VARCHAR(50),
    table_name NVARCHAR(256),
    schema_name NVARCHAR(128),
    operation VARCHAR(100),
    status VARCHAR(20),
    rows_processed BIGINT,
    duration_seconds INT,
    file_size_mb DECIMAL(10,2),
    error_message NVARCHAR(MAX),
    error_number INT,
    error_severity INT,
    log_timestamp DATETIME2 DEFAULT GETDATE(),
    execution_sequence INT
)
GO

CREATE PROCEDURE migration.sp_log_migration_event
    @Phase VARCHAR(50),
    @TableName NVARCHAR(256),
    @SchemaName NVARCHAR(128),
    @Operation VARCHAR(100),
    @Status VARCHAR(20),
    @RowsProcessed BIGINT = NULL,
    @DurationSeconds INT = NULL,
    @FileSizeMb DECIMAL(10,2) = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @ErrorNumber INT = NULL,
    @ErrorSeverity INT = NULL
AS
BEGIN
    BEGIN TRY
        INSERT INTO migration.migration_log 
        (phase, table_name, schema_name, operation, status, rows_processed, 
         duration_seconds, file_size_mb, error_message, error_number, error_severity, 
         execution_sequence)
        VALUES 
        (@Phase, @TableName, @SchemaName, @Operation, @Status, @RowsProcessed, 
         @DurationSeconds, @FileSizeMb, @ErrorMessage, @ErrorNumber, @ErrorSeverity,
         (SELECT ISNULL(MAX(execution_sequence), 0) + 1 FROM migration.migration_log 
          WHERE phase = @Phase AND operation = @Operation))
        
        PRINT 'Log entry created: ' + @Operation + ' on ' + @TableName + ' - ' + @Status
    END TRY
    BEGIN CATCH
        PRINT 'Error logging: ' + ERROR_MESSAGE()
    END CATCH
END
GO

-- ============================================================================
-- 2. DATA VALIDATION FRAMEWORK
-- ============================================================================

IF OBJECT_ID('migration.data_load_validation', 'U') IS NOT NULL
    DROP TABLE migration.data_load_validation
GO

CREATE TABLE migration.data_load_validation (
    validation_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    schema_name NVARCHAR(128),
    table_name NVARCHAR(256),
    source_row_count BIGINT,
    target_row_count BIGINT,
    row_count_match BIT,
    row_count_variance_pct DECIMAL(10,2),
    validation_status VARCHAR(20),
    validation_details NVARCHAR(MAX),
    validation_timestamp DATETIME2 DEFAULT GETDATE()
)
GO

CREATE PROCEDURE migration.sp_validate_load_counts
    @SourceConnectionString NVARCHAR(1024),
    @TargetConnectionString NVARCHAR(1024),
    @SourceDatabase NVARCHAR(256),
    @TargetDatabase NVARCHAR(256),
    @AccessToken NVARCHAR(MAX)
AS
BEGIN
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(256)
    DECLARE @source_count BIGINT, @target_count BIGINT
    DECLARE @variance_pct DECIMAL(10,2), @match BIT
    DECLARE @error_msg NVARCHAR(MAX)
    
    DECLARE table_cursor CURSOR FOR
    SELECT sc.name, tbl.name
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
    
    OPEN table_cursor
    FETCH NEXT FROM table_cursor INTO @schema, @table
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Get source count (current connection)
            DECLARE @source_sql NVARCHAR(MAX) = 
                'SELECT COUNT(*) FROM [' + @schema + '].[' + @table + ']'
            
            DECLARE @target_sql NVARCHAR(MAX) = 
                'SELECT COUNT(*) FROM [' + @schema + '].[' + @table + ']'
            
            -- Execute and get row counts
            -- Note: This requires separate connections for source/target
            
            SET @match = CASE WHEN @source_count = @target_count THEN 1 ELSE 0 END
            SET @variance_pct = CASE 
                WHEN @source_count = 0 THEN 0
                ELSE CAST((ABS(@source_count - @target_count) * 100.0) / @source_count AS DECIMAL(10,2))
            END
            
            INSERT INTO migration.data_load_validation
            (schema_name, table_name, source_row_count, target_row_count, 
             row_count_match, row_count_variance_pct, validation_status)
            VALUES 
            (@schema, @table, @source_count, @target_count, @match, 
             @variance_pct, CASE WHEN @match = 1 THEN 'PASS' ELSE 'FAIL' END)
            
            EXEC migration.sp_log_migration_event
                @Phase = 'VALIDATION',
                @TableName = @table,
                @SchemaName = @schema,
                @Operation = 'ROW_COUNT_VALIDATION',
                @Status = CASE WHEN @match = 1 THEN 'SUCCESS' ELSE 'MISMATCH' END,
                @RowsProcessed = @target_count
                
            PRINT 'Validated: ' + @schema + '.' + @table + 
                  ' (Source: ' + CAST(@source_count AS VARCHAR) + 
                  ' Target: ' + CAST(@target_count AS VARCHAR) + ')'
                  
        END TRY
        BEGIN CATCH
            SET @error_msg = ERROR_MESSAGE()
            INSERT INTO migration.data_load_validation
            (schema_name, table_name, validation_status, validation_details)
            VALUES (@schema, @table, 'ERROR', @error_msg)
            
            PRINT 'Validation error for ' + @schema + '.' + @table + ': ' + @error_msg
        END CATCH
        
        FETCH NEXT FROM table_cursor INTO @schema, @table
    END
    
    CLOSE table_cursor
    DEALLOCATE table_cursor
    
    -- Summary report
    SELECT 
        validation_status,
        COUNT(*) AS table_count
    FROM migration.data_load_validation
    GROUP BY validation_status
END
GO

-- ============================================================================
-- 3. POST-LOAD OPTIMIZATION PROCEDURES
-- ============================================================================

CREATE PROCEDURE migration.sp_update_table_statistics
    @SchemaName NVARCHAR(128) = NULL,
    @TableName NVARCHAR(256) = NULL
AS
BEGIN
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(256)
    DECLARE @sql NVARCHAR(MAX), @total_tables INT = 0, @updated INT = 0
    
    -- If specific table provided, update only that
    IF @SchemaName IS NOT NULL AND @TableName IS NOT NULL
    BEGIN
        SET @sql = 'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + ']'
        BEGIN TRY
            EXEC sp_executesql @sql
            SET @updated = 1
            PRINT 'Updated statistics for [' + @SchemaName + '].[' + @TableName + ']'
        END TRY
        BEGIN CATCH
            PRINT 'Error updating stats for [' + @SchemaName + '].[' + @TableName + ']: ' + ERROR_MESSAGE()
        END CATCH
    END
    ELSE
    BEGIN
        -- Update all tables
        DECLARE table_cursor CURSOR FOR
        SELECT sc.name, tbl.name
        FROM sys.tables tbl
        INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
        WHERE sc.name != 'migration' AND tbl.is_external = 0
        ORDER BY sc.name, tbl.name
        
        OPEN table_cursor
        FETCH NEXT FROM table_cursor INTO @schema, @table
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @sql = 'UPDATE STATISTICS [' + @schema + '].[' + @table + ']'
            BEGIN TRY
                EXEC sp_executesql @sql
                SET @updated = @updated + 1
                PRINT 'Updated statistics for [' + @schema + '].[' + @table + ']'
            END TRY
            BEGIN CATCH
                PRINT 'Skipped [' + @schema + '].[' + @table + ']: ' + ERROR_MESSAGE()
            END CATCH
            
            SET @total_tables = @total_tables + 1
            FETCH NEXT FROM table_cursor INTO @schema, @table
        END
        
        CLOSE table_cursor
        DEALLOCATE table_cursor
        
        PRINT 'Statistics update complete. Updated: ' + CAST(@updated AS VARCHAR) + 
              ' of ' + CAST(@total_tables AS VARCHAR) + ' tables'
    END
END
GO

-- ============================================================================
-- 4. TABLE SIZE AND PARTITIONING ANALYSIS
-- ============================================================================

IF OBJECT_ID('migration.table_size_analysis', 'U') IS NOT NULL
    DROP TABLE migration.table_size_analysis
GO

CREATE TABLE migration.table_size_analysis (
    analysis_id BIGINT IDENTITY(1,1),
    schema_name NVARCHAR(128),
    table_name NVARCHAR(256),
    row_count BIGINT,
    size_mb DECIMAL(15,2),
    size_gb DECIMAL(15,2),
    compression_recommended BIT,
    partitioning_recommended BIT,
    partition_key_suggestion NVARCHAR(256),
    analysis_timestamp DATETIME2 DEFAULT GETDATE()
)
GO

CREATE PROCEDURE migration.sp_analyze_table_sizes_and_partitions
AS
BEGIN
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(256), @row_count BIGINT, @size_mb DECIMAL(15,2)
    
    INSERT INTO migration.table_size_analysis
    (schema_name, table_name, row_count, size_mb, size_gb, 
     compression_recommended, partitioning_recommended, partition_key_suggestion)
    SELECT 
        sc.name,
        tbl.name,
        (SELECT SUM(ps.row_count) FROM sys.partitions ps WHERE ps.object_id = tbl.object_id),
        (SELECT SUM(ps.reserved_page_count) * 8 / 1024.0 
         FROM sys.dm_db_partition_stats ps WHERE ps.object_id = tbl.object_id),
        (SELECT SUM(ps.reserved_page_count) * 8 / 1024.0 / 1024.0 
         FROM sys.dm_db_partition_stats ps WHERE ps.object_id = tbl.object_id),
        CASE 
            WHEN (SELECT SUM(ps.reserved_page_count) * 8 / 1024.0 FROM sys.dm_db_partition_stats ps 
                  WHERE ps.object_id = tbl.object_id) > 5000 THEN 1 
            ELSE 0 
        END,
        CASE 
            WHEN (SELECT SUM(ps.reserved_page_count) * 8 / 1024.0 FROM sys.dm_db_partition_stats ps 
                  WHERE ps.object_id = tbl.object_id) > 10000 THEN 1 
            ELSE 0 
        END,
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = tbl.object_id 
                        AND name LIKE '%date%') THEN 'DateColumn'
            WHEN EXISTS (SELECT 1 FROM sys.columns WHERE object_id = tbl.object_id 
                        AND name LIKE '%id%') THEN 'IdColumn'
            ELSE NULL 
        END
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
    
    -- Display summary
    SELECT 
        'SUMMARY: Table Size Analysis' AS report_type,
        COUNT(*) AS total_tables,
        SUM(size_gb) AS total_size_gb,
        SUM(CASE WHEN compression_recommended = 1 THEN 1 ELSE 0 END) AS tables_needing_compression,
        SUM(CASE WHEN partitioning_recommended = 1 THEN 1 ELSE 0 END) AS tables_needing_partition
    FROM migration.table_size_analysis
    WHERE analysis_timestamp = (SELECT MAX(analysis_timestamp) FROM migration.table_size_analysis)
    
    -- Display detailed recommendations
    SELECT 
        schema_name,
        table_name,
        row_count,
        size_gb,
        CASE 
            WHEN partitioning_recommended = 1 THEN 'CRITICAL: Partition on ' + ISNULL(partition_key_suggestion, 'DateColumn')
            WHEN compression_recommended = 1 THEN 'RECOMMENDED: Enable compression'
            ELSE 'No immediate action needed'
        END AS recommendation
    FROM migration.table_size_analysis
    WHERE analysis_timestamp = (SELECT MAX(analysis_timestamp) FROM migration.table_size_analysis)
    ORDER BY size_gb DESC
END
GO

-- ============================================================================
-- 5. DATA TYPE COMPATIBILITY CHECK
-- ============================================================================

CREATE PROCEDURE migration.sp_check_unsupported_datatypes
AS
BEGIN
    DECLARE @unsupported_found INT = 0
    
    SELECT 
        @unsupported_found = COUNT(*)
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    INNER JOIN sys.columns col ON tbl.object_id = col.object_id
    INNER JOIN sys.types tp ON col.system_type_id = tp.system_type_id AND col.user_type_id = tp.user_type_id
    WHERE sc.name != 'migration'
    AND tp.name IN ('geometry', 'geography', 'hierarchyid', 'image', 'text', 'ntext')
    
    IF @unsupported_found > 0
    BEGIN
        PRINT '=== UNSUPPORTED DATA TYPES FOUND ==='
        SELECT 
            sc.name AS schema_name,
            tbl.name AS table_name,
            col.name AS column_name,
            tp.name AS data_type,
            CASE 
                WHEN tp.name = 'geometry' THEN 'Convert to binary or WKT string'
                WHEN tp.name = 'geography' THEN 'Convert to binary or WKT string'
                WHEN tp.name = 'hierarchyid' THEN 'Convert to string'
                WHEN tp.name IN ('image', 'text', 'ntext') THEN 'Convert to binary/varchar(max)'
                ELSE 'Review compatibility'
            END AS recommended_action
        FROM sys.tables tbl
        INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
        INNER JOIN sys.columns col ON tbl.object_id = col.object_id
        INNER JOIN sys.types tp ON col.system_type_id = tp.system_type_id
        WHERE sc.name != 'migration'
        AND tp.name IN ('geometry', 'geography', 'hierarchyid', 'image', 'text', 'ntext')
        ORDER BY sc.name, tbl.name, col.name
    END
    ELSE
    BEGIN
        PRINT '=== ALL DATA TYPES COMPATIBLE WITH FABRIC ==='
    END
END
GO

-- ============================================================================
-- 6. MIGRATION PROGRESS REPORT
-- ============================================================================

CREATE PROCEDURE migration.sp_get_migration_progress_report
AS
BEGIN
    PRINT '====== MIGRATION PROGRESS REPORT ======'
    PRINT 'Generated: ' + CAST(GETDATE() AS VARCHAR)
    PRINT ''
    
    -- Overall statistics
    SELECT 
        'TOTAL TABLES' AS metric,
        CAST(COUNT(*) AS VARCHAR) AS value
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
    
    UNION ALL
    
    SELECT 
        'EXTRACTION_COMPLETED',
        CAST(COUNT(DISTINCT table_name) AS VARCHAR)
    FROM migration.migration_log
    WHERE phase = 'EXTRACTION' AND status = 'SUCCESS'
    
    UNION ALL
    
    SELECT 
        'LOAD_COMPLETED',
        CAST(COUNT(DISTINCT table_name) AS VARCHAR)
    FROM migration.migration_log
    WHERE phase = 'LOADING' AND status = 'SUCCESS'
    
    UNION ALL
    
    SELECT 
        'VALIDATION_PASSED',
        CAST(COUNT(*) AS VARCHAR)
    FROM migration.data_load_validation
    WHERE validation_status = 'PASS'
    
    PRINT ''
    PRINT '====== FAILED OPERATIONS ======'
    SELECT 
        phase,
        table_name,
        operation,
        COUNT(*) AS failure_count,
        MAX(log_timestamp) AS last_attempt
    FROM migration.migration_log
    WHERE status != 'SUCCESS'
    GROUP BY phase, table_name, operation
    
    PRINT ''
    PRINT '====== EXTRACTION PERFORMANCE ======'
    SELECT TOP 10
        table_name,
        file_size_mb,
        duration_seconds,
        CAST((file_size_mb / NULLIF(duration_seconds, 0)) AS DECIMAL(10,2)) AS throughput_mb_per_sec
    FROM migration.migration_log
    WHERE phase = 'EXTRACTION' AND status = 'SUCCESS' AND file_size_mb > 0
    ORDER BY throughput_mb_per_sec DESC
    
    PRINT ''
    PRINT '====== LOAD PERFORMANCE ======'
    SELECT TOP 10
        table_name,
        rows_processed,
        duration_seconds,
        CAST((rows_processed / NULLIF(duration_seconds, 0)) AS BIGINT) AS rows_per_sec
    FROM migration.migration_log
    WHERE phase = 'LOADING' AND status = 'SUCCESS' AND rows_processed > 0
    ORDER BY rows_per_sec DESC
END
GO

-- ============================================================================
-- 7. ENHANCED COPY INTO WITH RETRY LOGIC
-- ============================================================================

IF OBJECT_ID('migration.sp_generate_copy_into_with_retry', 'U') IS NOT NULL
    DROP PROCEDURE migration.sp_generate_copy_into_with_retry
GO

CREATE PROCEDURE migration.sp_generate_copy_into_with_retry
    @storage_access_token VARCHAR(1024),
    @external_data_source_base_location VARCHAR(1024),
    @max_retries INT = 3
AS
BEGIN
    DECLARE @schema NVARCHAR(128), @table NVARCHAR(256)
    
    -- Generate COPY INTO statements with built-in retry logic
    SELECT 
        sc.name AS SchName,
        tbl.name AS objName,
        'DECLARE @retry INT = 0
DECLARE @max_retries INT = ' + CAST(@max_retries AS VARCHAR) + '

WHILE @retry <= @max_retries
BEGIN
    BEGIN TRY
        COPY INTO [' + sc.name + '].[' + tbl.name + '] 
        FROM ''' + @external_data_source_base_location + '' + sc.name + '/' + tbl.name + '/''' +
        ' WITH ( 
            FILE_TYPE = ''PARQUET'', 
            CREDENTIAL=(IDENTITY= ''Storage Account Key'', SECRET = ''' + @storage_access_token + '''),
            MAXERRORS = 1000,
            COMPRESSION = ''SNAPPY''
        )
        
        EXEC migration.sp_log_migration_event
            @Phase = ''LOADING'',
            @TableName = ''' + tbl.name + ''',
            @SchemaName = ''' + sc.name + ''',
            @Operation = ''COPY_INTO'',
            @Status = ''SUCCESS'',
            @DurationSeconds = DATEDIFF(SECOND, @load_start, GETDATE())
        
        SET @retry = @max_retries + 1 -- Exit loop on success
    END TRY
    BEGIN CATCH
        IF @retry < @max_retries
        BEGIN
            SET @retry = @retry + 1
            WAITFOR DELAY ''00:00:' + CAST(POWER(2, @max_retries - 1) AS VARCHAR) + ''' -- Exponential backoff
            PRINT ''Retry ' + CAST(POWER(2, @max_retries - 1) AS VARCHAR) + ' for [' + sc.name + '].[' + tbl.name + ']''
        END
        ELSE
        BEGIN
            EXEC migration.sp_log_migration_event
                @Phase = ''LOADING'',
                @TableName = ''' + tbl.name + ''',
                @SchemaName = ''' + sc.name + ''',
                @Operation = ''COPY_INTO'',
                @Status = ''FAILED'',
                @ErrorMessage = ERROR_MESSAGE(),
                @ErrorNumber = ERROR_NUMBER(),
                @ErrorSeverity = ERROR_SEVERITY()
            
            THROW
        END
    END CATCH
END' AS data_load_statement_with_retry
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
    ORDER BY sc.name, tbl.name
END
GO

-- ============================================================================
-- INITIALIZATION: Run these procedures to set up the framework
-- ============================================================================

PRINT 'Migration optimization framework deployed successfully!'
PRINT ''
PRINT 'Available procedures:'
PRINT '  - migration.sp_log_migration_event: Log events during migration'
PRINT '  - migration.sp_validate_load_counts: Validate row counts post-load'
PRINT '  - migration.sp_update_table_statistics: Update table statistics'
PRINT '  - migration.sp_analyze_table_sizes_and_partitions: Analyze table structure'
PRINT '  - migration.sp_check_unsupported_datatypes: Check data type compatibility'
PRINT '  - migration.sp_get_migration_progress_report: Get progress summary'
PRINT '  - migration.sp_generate_copy_into_with_retry: Enhanced COPY INTO with retry'
PRINT ''
PRINT 'Next steps:'
PRINT '  1. EXEC migration.sp_check_unsupported_datatypes'
PRINT '  2. EXEC migration.sp_analyze_table_sizes_and_partitions'
PRINT '  3. Use logging procedures during migration'
PRINT '  4. EXEC migration.sp_get_migration_progress_report for status'
