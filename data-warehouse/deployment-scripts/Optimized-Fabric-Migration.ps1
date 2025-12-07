#!/usr/bin/env pwsh

<#
.SYNOPSIS
    Enhanced Fabric Migration Script with Parallel Processing and Optimization
    
.DESCRIPTION
    This script optimizes the migration from Azure Synapse Dedicated Pool to Fabric Warehouse
    with parallel COPY INTO operations, comprehensive logging, and performance monitoring.
    
.PARAMETER ServerInstance
    Source Synapse Dedicated SQL Pool server name
    
.PARAMETER Database
    Source database name
    
.PARAMETER StorageAccessToken
    Azure Storage account access key or SAS token
    
.PARAMETER AdlsLocation
    ADLS Gen2 base location (abfss://container@account.dfs.core.windows.net/)
    
.PARAMETER TargetServerName
    Fabric SQL Analytics endpoint
    
.PARAMETER TargetDatabase
    Target Fabric database
    
.PARAMETER MaxParallelLoads
    Maximum number of parallel COPY INTO operations (default: 4)
    
.PARAMETER EnableValidation
    Enable post-load row count validation (default: $true)
    
.PARAMETER EnableStatisticsUpdate
    Update table statistics post-load (default: $true)

.EXAMPLE
    .\Optimized-Fabric-Migration.ps1 `
        -ServerInstance "mysynapse.sql.azuresynapse.net" `
        -Database "mydb" `
        -StorageAccessToken "storage_key" `
        -AdlsLocation "abfss://container@account.dfs.core.windows.net/" `
        -TargetServerName "fabric.fabric.microsoft.com" `
        -TargetDatabase "fabric_db" `
        -MaxParallelLoads 4
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$ServerInstance,

    [Parameter(Mandatory = $true)]
    [string]$Database,

    [Parameter(Mandatory = $true)]
    [string]$StorageAccessToken,

    [Parameter(Mandatory = $true)]
    [string]$AdlsLocation,

    [Parameter(Mandatory = $true)]
    [string]$TargetServerName,

    [Parameter(Mandatory = $true)]
    [string]$TargetDatabase,

    [Parameter(Mandatory = $false)]
    [int]$MaxParallelLoads = 4,

    [Parameter(Mandatory = $false)]
    [bool]$EnableValidation = $true,

    [Parameter(Mandatory = $false)]
    [bool]$EnableStatisticsUpdate = $true,

    [Parameter(Mandatory = $false)]
    [string]$LogPath = ".\migration_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
)

# ============================================================================
# INITIALIZATION
# ============================================================================

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# Setup logging
$logPath = $LogPath
$logFolder = Split-Path -Parent $logPath
if (-not (Test-Path $logFolder)) {
    mkdir $logFolder | Out-Null
}

function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    
    Add-Content -Path $logPath -Value $logMessage -ErrorAction SilentlyContinue
    
    switch ($Level) {
        "ERROR" { Write-Host $logMessage -ForegroundColor Red }
        "WARNING" { Write-Host $logMessage -ForegroundColor Yellow }
        "SUCCESS" { Write-Host $logMessage -ForegroundColor Green }
        default { Write-Host $logMessage }
    }
}

Write-Log "=== FABRIC MIGRATION - OPTIMIZED SCRIPT STARTED ===" "SUCCESS"
Write-Log "Source: $ServerInstance / $Database" "INFO"
Write-Log "Target: $TargetServerName / $TargetDatabase" "INFO"
Write-Log "Log file: $logPath" "INFO"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

function Connect-SqlDatabase {
    param(
        [string]$ServerInstance,
        [string]$Database,
        [string]$AccessToken
    )
    
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = "Server=$ServerInstance;Initial Catalog=$Database;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30"
    $connection.AccessToken = $AccessToken
    
    try {
        $connection.Open()
        Write-Log "Connected to $ServerInstance / $Database" "SUCCESS"
        return $connection
    }
    catch {
        Write-Log "Failed to connect: $_" "ERROR"
        throw
    }
}

function Execute-SqlQuery {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$Query,
        [int]$CommandTimeout = 300
    )
    
    $command = $Connection.CreateCommand()
    $command.CommandText = $Query
    $command.CommandTimeout = $CommandTimeout
    
    try {
        $result = $command.ExecuteReader()
        $table = New-Object System.Data.DataTable
        $table.Load($result)
        return $table
    }
    catch {
        Write-Log "SQL Execution Error: $_" "ERROR"
        throw
    }
    finally {
        if ($result) { $result.Dispose() }
        if ($command) { $command.Dispose() }
    }
}

function Get-MigrationTables {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection
    )
    
    $query = @"
    SELECT 
        sc.name AS SchemaName,
        tbl.name AS TableName,
        (SELECT SUM(ps.row_count) FROM sys.partitions ps WHERE ps.object_id = tbl.object_id) AS RowCount,
        (SELECT SUM(ps.reserved_page_count) * 8 / 1024.0 FROM sys.dm_db_partition_stats ps WHERE ps.object_id = tbl.object_id) AS SizeMb
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
    ORDER BY SizeMb DESC
"@
    
    return Execute-SqlQuery -Connection $Connection -Query $query
}

function Generate-CopyIntoStatement {
    param(
        [string]$SchemaName,
        [string]$TableName,
        [string]$AdlsPath,
        [string]$StorageToken,
        [int]$MaxRetries = 3
    )
    
    $escapedToken = $StorageToken.Replace("'", "''")
    
    @"
DECLARE @retry INT = 0
DECLARE @max_retries INT = $MaxRetries
DECLARE @retry_delay INT = 5

WHILE @retry <= @max_retries
BEGIN
    BEGIN TRY
        PRINT 'Loading [$SchemaName].[$TableName] (Attempt ' + CAST(@retry + 1 AS VARCHAR) + ')'
        
        COPY INTO [$SchemaName].[$TableName] 
        FROM '$AdlsPath$SchemaName/$TableName/'
        WITH ( 
            FILE_TYPE = 'PARQUET', 
            CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '$escapedToken'),
            MAXERRORS = 1000
        )
        
        PRINT 'Successfully loaded [$SchemaName].[$TableName]'
        SET @retry = @max_retries + 1
    END TRY
    BEGIN CATCH
        IF @retry < @max_retries
        BEGIN
            SET @retry = @retry + 1
            PRINT 'Retry ' + CAST(@retry AS VARCHAR) + ' for [$SchemaName].[$TableName] in ' + CAST(@retry_delay AS VARCHAR) + ' seconds'
            WAITFOR DELAY '00:00:' + CAST(@retry_delay AS VARCHAR)
            SET @retry_delay = @retry_delay * 2 -- Exponential backoff
        END
        ELSE
        BEGIN
            PRINT 'ERROR: Failed to load [$SchemaName].[$TableName] after ' + CAST(@max_retries AS VARCHAR) + ' retries'
            THROW
        END
    END CATCH
END
"@
}

# ============================================================================
# MAIN MIGRATION FLOW
# ============================================================================

function Start-OptimizedMigration {
    param(
        [string]$SourceServer,
        [string]$SourceDatabase,
        [string]$TargetServer,
        [string]$TargetDatabase,
        [string]$StorageToken,
        [string]$AdlsPath,
        [int]$MaxParallel
    )
    
    # Get access tokens
    Write-Log "Acquiring authentication tokens..." "INFO"
    Connect-AzAccount -ErrorAction SilentlyContinue | Out-Null
    
    $sourceToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net/).Token
    $targetToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net/).Token
    
    # Connect to source
    Write-Log "Connecting to source Synapse..." "INFO"
    $sourceConnection = Connect-SqlDatabase -ServerInstance $SourceServer -Database $SourceDatabase -AccessToken $sourceToken
    
    # Connect to target
    Write-Log "Connecting to target Fabric..." "INFO"
    $targetConnection = Connect-SqlDatabase -ServerInstance $TargetServer -Database $TargetDatabase -AccessToken $targetToken
    
    try {
        # Get list of tables to migrate
        Write-Log "Retrieving table list from source..." "INFO"
        $tables = Get-MigrationTables -Connection $sourceConnection
        Write-Log "Found $($tables.Rows.Count) tables to migrate" "SUCCESS"
        
        # Generate COPY INTO statements
        Write-Log "Generating COPY INTO statements..." "INFO"
        $copyStatements = @()
        
        foreach ($row in $tables.Rows) {
            $statement = Generate-CopyIntoStatement `
                -SchemaName $row.SchemaName `
                -TableName $row.TableName `
                -AdlsPath $AdlsPath `
                -StorageToken $StorageToken
            
            $copyStatements += @{
                SchemaName = $row.SchemaName
                TableName = $row.TableName
                RowCount = $row.RowCount
                SizeMb = $row.SizeMb
                Statement = $statement
            }
        }
        
        # Execute COPY INTO operations with parallelism
        Write-Log "Starting parallel COPY INTO operations (Max concurrent: $MaxParallel)..." "SUCCESS"
        Start-ParallelCopyInto -CopyStatements $copyStatements -TargetConnection $targetConnection -MaxParallel $MaxParallel
        
        # Post-load operations
        if ($EnableValidation) {
            Write-Log "Validating data loads..." "INFO"
            Validate-LoadedData -SourceConnection $sourceConnection -TargetConnection $targetConnection
        }
        
        if ($EnableStatisticsUpdate) {
            Write-Log "Updating statistics..." "INFO"
            Update-TargetStatistics -TargetConnection $targetConnection
        }
        
        Write-Log "=== MIGRATION COMPLETED SUCCESSFULLY ===" "SUCCESS"
        
    }
    finally {
        $sourceConnection.Close()
        $targetConnection.Close()
        Write-Log "Connections closed" "INFO"
    }
}

function Start-ParallelCopyInto {
    param(
        [array]$CopyStatements,
        [System.Data.SqlClient.SqlConnection]$TargetConnection,
        [int]$MaxParallel
    )
    
    $jobs = @()
    $completedTables = 0
    $failedTables = @()
    
    foreach ($statement in $CopyStatements) {
        # Wait if max parallel jobs reached
        while ((Get-Job -State Running -ErrorAction SilentlyContinue | Measure-Object).Count -ge $MaxParallel) {
            Start-Sleep -Milliseconds 500
        }
        
        # Check for completed jobs
        $completedJobs = Get-Job -State Completed -ErrorAction SilentlyContinue
        foreach ($job in $completedJobs) {
            try {
                $result = Receive-Job -Job $job -ErrorAction Stop
                $completedTables++
                Write-Log "Completed: $($job.Name)" "SUCCESS"
            }
            catch {
                Write-Log "Failed: $($job.Name) - $_" "ERROR"
                $failedTables += $job.Name
            }
            Remove-Job -Job $job
        }
        
        # Start new job
        $jobName = "$($statement.SchemaName).$($statement.TableName)"
        $job = Start-Job -Name $jobName -ScriptBlock {
            param($sql, $server, $db, $token)
            
            $conn = New-Object System.Data.SqlClient.SqlConnection
            $conn.ConnectionString = "Server=$server;Initial Catalog=$db;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30"
            $conn.AccessToken = $token
            $conn.Open()
            
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = $sql
            $cmd.CommandTimeout = 0
            $cmd.ExecuteNonQuery()
            
            $conn.Close()
        } -ArgumentList $statement.Statement, $TargetConnection.DataSource, $TargetConnection.Database, (Get-AzAccessToken -ResourceUrl https://database.windows.net/).Token
        
        Write-Log "Started loading: $jobName (Size: $($statement.SizeMb) MB, Rows: $($statement.RowCount))" "INFO"
    }
    
    # Wait for remaining jobs
    Write-Log "Waiting for all jobs to complete..." "INFO"
    Get-Job | Wait-Job
    
    # Collect final results
    $completedJobs = Get-Job -State Completed
    foreach ($job in $completedJobs) {
        try {
            Receive-Job -Job $job -ErrorAction Stop | Out-Null
            $completedTables++
            Write-Log "Final: $($job.Name)" "SUCCESS"
        }
        catch {
            Write-Log "Final Failed: $($job.Name) - $_" "ERROR"
            $failedTables += $job.Name
        }
        Remove-Job -Job $job
    }
    
    Write-Log "Parallel load summary - Completed: $completedTables, Failed: $($failedTables.Count)" "SUCCESS"
    if ($failedTables.Count -gt 0) {
        Write-Log "Failed tables: $($failedTables -join ', ')" "WARNING"
    }
}

function Validate-LoadedData {
    param(
        [System.Data.SqlClient.SqlConnection]$SourceConnection,
        [System.Data.SqlClient.SqlConnection]$TargetConnection
    )
    
    $query = @"
    SELECT sc.name, tbl.name
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
"@
    
    $tables = Execute-SqlQuery -Connection $SourceConnection -Query $query
    
    $passCount = 0
    $failCount = 0
    
    foreach ($row in $tables.Rows) {
        $schema = $row[0]
        $table = $row[1]
        
        # Get source count
        $sourceCountQuery = "SELECT COUNT(*) FROM [$schema].[$table]"
        $sourceResult = Execute-SqlQuery -Connection $SourceConnection -Query $sourceCountQuery
        $sourceCount = $sourceResult.Rows[0][0]
        
        # Get target count
        $targetCountQuery = "SELECT COUNT(*) FROM [$schema].[$table]"
        $targetResult = Execute-SqlQuery -Connection $TargetConnection -Query $targetCountQuery
        $targetCount = $targetResult.Rows[0][0]
        
        if ($sourceCount -eq $targetCount) {
            $passCount++
            Write-Log "Validation PASS: [$schema].[$table] ($sourceCount rows)" "SUCCESS"
        }
        else {
            $failCount++
            Write-Log "Validation FAIL: [$schema].[$table] (Source: $sourceCount, Target: $targetCount)" "ERROR"
        }
    }
    
    Write-Log "Validation Summary - Passed: $passCount, Failed: $failCount" "SUCCESS"
}

function Update-TargetStatistics {
    param(
        [System.Data.SqlClient.SqlConnection]$TargetConnection
    )
    
    $query = @"
    SELECT sc.name, tbl.name
    FROM sys.tables tbl
    INNER JOIN sys.schemas sc ON tbl.schema_id = sc.schema_id
    WHERE sc.name != 'migration' AND tbl.is_external = 0
"@
    
    $tables = Execute-SqlQuery -Connection $TargetConnection -Query $query
    
    $updatedCount = 0
    foreach ($row in $tables.Rows) {
        $schema = $row[0]
        $table = $row[1]
        
        $updateQuery = "UPDATE STATISTICS [$schema].[$table]"
        try {
            Execute-SqlQuery -Connection $TargetConnection -Query $updateQuery | Out-Null
            $updatedCount++
            Write-Log "Updated statistics: [$schema].[$table]" "SUCCESS"
        }
        catch {
            Write-Log "Failed to update statistics for [$schema].[$table]: $_" "WARNING"
        }
    }
    
    Write-Log "Updated statistics for $updatedCount tables" "SUCCESS"
}

# ============================================================================
# EXECUTION
# ============================================================================

try {
    Start-OptimizedMigration `
        -SourceServer $ServerInstance `
        -SourceDatabase $Database `
        -TargetServer $TargetServerName `
        -TargetDatabase $TargetDatabase `
        -StorageToken $StorageAccessToken `
        -AdlsPath $AdlsLocation `
        -MaxParallel $MaxParallelLoads
}
catch {
    Write-Log "CRITICAL ERROR: $_" "ERROR"
    Write-Log "Stack trace: $($_.ScriptStackTrace)" "ERROR"
    exit 1
}

Write-Log "Script execution completed. Check log file: $logPath" "SUCCESS"
