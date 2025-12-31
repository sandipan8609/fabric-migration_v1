# Azure Dedicated Pool to Microsoft Fabric Warehouse Migration Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Migration Architecture](#migration-architecture)
4. [Pre-Migration Checklist](#pre-migration-checklist)
5. [Migration Steps](#migration-steps)
6. [Post-Migration Validation](#post-migration-validation)
7. [Troubleshooting](#troubleshooting)
8. [Performance Optimization](#performance-optimization)

---

## Overview

This guide provides a comprehensive step-by-step approach for migrating data from **Azure Synapse Analytics Dedicated SQL Pool** to **Microsoft Fabric Warehouse** using a two-phase approach:

1. **Phase 1**: Extract data from Azure Dedicated Pool to Azure Data Lake Storage (ADLS) Gen2 using CETAS (CREATE EXTERNAL TABLE AS SELECT)
2. **Phase 2**: Load data from ADLS Gen2 to Fabric Warehouse using COPY INTO

### Key Benefits of This Approach
- **No direct connection required** between source and target
- **Cost-effective**: Pay only for storage during transition
- **Flexible**: Can pause/resume migration
- **Parallel processing**: Extract and load tables concurrently
- **Data validation**: Verify row counts at each stage

### Migration Timeline
| Database Size | Estimated Time | Recommended Approach |
|--------------|----------------|---------------------|
| < 10 GB | 30 min - 1 hour | Sequential processing |
| 10-100 GB | 1-4 hours | Parallel processing (4-6 threads) |
| 100 GB - 1 TB | 4-12 hours | Parallel processing (8-12 threads) |
| > 1 TB | 12-48 hours | Partitioned + parallel processing |

---

## Prerequisites

### 1. Azure Resources
- ✅ **Azure Synapse Analytics Dedicated SQL Pool** (source)
- ✅ **Azure Data Lake Storage Gen2** (staging area)
- ✅ **Microsoft Fabric Workspace** with Warehouse enabled
- ✅ **Azure Service Principal** or **Managed Identity** for authentication

### 2. Required Software
```bash
# Python 3.8 or higher
python3 --version

# Azure CLI
az --version

# pip packages
pip install azure-identity azure-storage-blob pyodbc pandas sqlalchemy
```

### 3. Network Access
- Source Synapse pool accessible from migration environment
- ADLS Gen2 accessible from both source and target
- Fabric Warehouse accessible from migration environment

### 4. Permissions
Refer to [PERMISSIONS_GUIDE.md](./PERMISSIONS_GUIDE.md) for detailed permission requirements.

---

## Migration Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     MIGRATION ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────┘

                            PHASE 1: EXTRACT
┌──────────────────────┐         CETAS          ┌─────────────────┐
│                      │   ─────────────────>   │                 │
│  Azure Dedicated     │    (Parquet Files)     │  ADLS Gen2      │
│  SQL Pool (Source)   │                        │  Storage        │
│                      │                        │                 │
└──────────────────────┘                        └─────────────────┘
         │                                              │
         │                                              │
         │      Python Migration Script                 │
         │      - Table discovery                       │
         │      - Parallel extraction                   │
         │      - Progress tracking                     │
         │      - Error handling                        │
         │                                              │
         └──────────────────────────────────────────────┘
                            
                            PHASE 2: LOAD
                        ┌─────────────────┐
                        │  ADLS Gen2      │
                        │  Storage        │
                        └─────────────────┘
                                │
                                │ COPY INTO
                                │ (Parallel)
                                ▼
                        ┌─────────────────┐
                        │                 │
                        │  Microsoft      │
                        │  Fabric         │
                        │  Warehouse      │
                        │  (Target)       │
                        │                 │
                        └─────────────────┘
```

---

## Pre-Migration Checklist

### 1. Assessment Phase (1-2 hours)

#### A. Analyze Source Database
```sql
-- Run on Azure Dedicated Pool
-- Get database size
SELECT 
    SUM(reserved_space_GB) as total_size_gb,
    COUNT(DISTINCT table_name) as table_count
FROM (
    SELECT 
        s.name + '.' + t.name as table_name,
        SUM(ps.reserved_page_count) * 8.0 / 1024 / 1024 as reserved_space_GB
    FROM sys.dm_pdw_nodes_db_partition_stats ps
    INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id AND ps.pdw_node_id = nt.pdw_node_id
    INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
    INNER JOIN sys.tables t ON tm.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    GROUP BY s.name, t.name
) subq;

-- Get table list with row counts
SELECT 
    s.name as schema_name,
    t.name as table_name,
    SUM(ps.row_count) as row_count,
    SUM(ps.reserved_page_count) * 8.0 / 1024 as size_mb
FROM sys.dm_pdw_nodes_db_partition_stats ps
INNER JOIN sys.pdw_nodes_tables nt ON ps.object_id = nt.object_id AND ps.pdw_node_id = nt.pdw_node_id
INNER JOIN sys.pdw_table_mappings tm ON nt.name = tm.physical_name
INNER JOIN sys.tables t ON tm.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
GROUP BY s.name, t.name
ORDER BY size_mb DESC;
```

#### B. Check for Unsupported Features
```sql
-- Run the unsupported features check script
EXEC migration.Check_UnsupportedDML;
```

#### C. Identify Datatype Issues
```sql
-- Check for unsupported datatypes
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    c.name AS column_name,
    tp.name AS data_type,
    c.max_length,
    c.precision,
    c.scale
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND tp.name IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 'hierarchyid', 'geometry', 'geography')
ORDER BY s.name, t.name, c.name;
```

### 2. Environment Setup (30 minutes)

#### A. Create Storage Account and Container
```bash
#!/bin/bash
# setup_storage.sh

# Variables
RESOURCE_GROUP="migration-rg"
LOCATION="eastus"
STORAGE_ACCOUNT="migrationstg$(date +%s)"
CONTAINER_NAME="migration-staging"

# Create resource group (if not exists)
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create storage account with hierarchical namespace enabled
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hierarchical-namespace true

# Create container
az storage container create \
    --name $CONTAINER_NAME \
    --account-name $STORAGE_ACCOUNT \
    --auth-mode login

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
    --account-name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query '[0].value' -o tsv)

echo "Storage Account: $STORAGE_ACCOUNT"
echo "Container: $CONTAINER_NAME"
echo "Storage Key: $STORAGE_KEY"
```

#### B. Setup Service Principal (if not using Managed Identity)
```bash
#!/bin/bash
# setup_service_principal.sh

# Create service principal
SP_NAME="fabric-migration-sp"
SP_OUTPUT=$(az ad sp create-for-rbac --name $SP_NAME --role Contributor)

# Extract credentials
CLIENT_ID=$(echo $SP_OUTPUT | jq -r '.appId')
CLIENT_SECRET=$(echo $SP_OUTPUT | jq -r '.password')
TENANT_ID=$(echo $SP_OUTPUT | jq -r '.tenant')

echo "Service Principal Created:"
echo "Client ID: $CLIENT_ID"
echo "Client Secret: $CLIENT_SECRET"
echo "Tenant ID: $TENANT_ID"

# Save to environment file
cat > .env << EOF
AZURE_TENANT_ID=$TENANT_ID
AZURE_CLIENT_ID=$CLIENT_ID
AZURE_CLIENT_SECRET=$CLIENT_SECRET
EOF

echo "Credentials saved to .env file"
```

#### C. Configure Source Database
```sql
-- Run on Azure Dedicated Pool
-- Create migration schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'migration')
BEGIN
    EXEC('CREATE SCHEMA migration')
END
GO

-- Deploy optimization framework
-- Run the optimization_framework.sql script
```

### 3. Validation Phase (15 minutes)

#### A. Test Connectivity
```python
# test_connectivity.py
import pyodbc
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def test_synapse_connection(server, database):
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Authentication=ActiveDirectoryInteractive"
        conn = pyodbc.connect(conn_str)
        print("✅ Synapse connection successful")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Synapse connection failed: {e}")
        return False

def test_storage_connection(account_name):
    try:
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=credential
        )
        blob_service_client.get_account_information()
        print("✅ Storage account connection successful")
        return True
    except Exception as e:
        print(f"❌ Storage connection failed: {e}")
        return False

def test_fabric_connection(workspace, warehouse):
    try:
        server = f"{workspace}.datawarehouse.fabric.microsoft.com"
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={warehouse};Authentication=ActiveDirectoryInteractive"
        conn = pyodbc.connect(conn_str)
        print("✅ Fabric Warehouse connection successful")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Fabric connection failed: {e}")
        return False

# Test all connections
if __name__ == "__main__":
    test_synapse_connection("mysynapse.sql.azuresynapse.net", "mydatabase")
    test_storage_connection("mystorageaccount")
    test_fabric_connection("myworkspace", "mywarehouse")
```

---

## Migration Steps

### Step 1: Extract Data from Azure Dedicated Pool (Phase 1)

#### A. Create External Data Source and File Format
```sql
-- Run on Azure Dedicated Pool
-- Create master key (if not exists)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword!123';
GO

-- Create database scoped credential
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '<your_sas_token>';
GO

-- Create external data source
CREATE EXTERNAL DATA SOURCE MigrationStaging
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://migration-staging@yourstorageaccount.dfs.core.windows.net',
    CREDENTIAL = AzureStorageCredential
);
GO

-- Create external file format
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO
```

#### B. Run Python Extraction Script
```bash
# Extract data using Python script
python3 scripts/extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6 \
    --batch-size 50
```

### Step 2: Load Data to Fabric Warehouse (Phase 2)

#### A. Create Schemas in Fabric Warehouse
```sql
-- Run on Fabric Warehouse
-- Create schemas from source
CREATE SCHEMA dbo;
CREATE SCHEMA sales;
CREATE SCHEMA staging;
-- Add other schemas as needed
```

#### B. Run Python Loading Script
```bash
# Load data using Python script
python3 scripts/load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows
```

### Step 3: Post-Load Operations

#### A. Create Statistics
```sql
-- Run on Fabric Warehouse
-- Update statistics for all tables
EXEC migration.sp_update_table_statistics;
```

#### B. Validate Data
```bash
# Run validation script
python3 scripts/validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

---

## Post-Migration Validation

### 1. Row Count Validation
```sql
-- Compare row counts between source and target
-- Run on Fabric Warehouse
SELECT 
    source_schema,
    source_table,
    source_row_count,
    target_row_count,
    CASE 
        WHEN source_row_count = target_row_count THEN '✅ Match'
        ELSE '❌ Mismatch'
    END as validation_status,
    ABS(source_row_count - target_row_count) as row_difference
FROM migration.data_load_validation
ORDER BY validation_status, source_schema, source_table;
```

### 2. Data Type Validation
```sql
-- Verify data types are correctly mapped
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
```

### 3. Performance Validation
```sql
-- Run sample queries to test performance
-- Example: Top 10 tables by size
SELECT TOP 10
    SCHEMA_NAME(t.schema_id) AS schema_name,
    t.name AS table_name,
    SUM(p.rows) AS row_count
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
    AND SCHEMA_NAME(t.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
GROUP BY t.schema_id, t.name
ORDER BY row_count DESC;
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: CETAS Fails with Permission Error
```
Error: External table creation failed
Solution:
1. Verify Managed Identity has "Storage Blob Data Contributor" role on storage account
2. Check firewall rules allow access from Synapse
3. Verify SAS token is valid and has read/write permissions
```

#### Issue 2: COPY INTO Fails with Timeout
```
Error: Operation timeout exceeded
Solution:
1. Break large tables into smaller chunks
2. Increase partition size in extraction
3. Use parallel COPY INTO operations
4. Check Fabric Warehouse capacity units
```

#### Issue 3: Row Count Mismatch
```
Error: Source and target row counts don't match
Solution:
1. Check for duplicate extraction files
2. Verify no data was inserted/deleted during migration
3. Re-extract and reload the affected table
4. Check for NULL values in key columns
```

#### Issue 4: Unsupported Data Type
```
Error: Data type not supported in Fabric Warehouse
Solution:
1. Refer to DATATYPE_MAPPING.md for conversion
2. Convert problematic columns before extraction
3. Use VARCHAR/NVARCHAR for complex types
4. Handle XML/JSON as text
```

---

## Performance Optimization

### 1. Parallel Processing
- **Small databases (< 10 GB)**: 2-4 parallel jobs
- **Medium databases (10-100 GB)**: 4-8 parallel jobs
- **Large databases (> 100 GB)**: 8-16 parallel jobs

### 2. Table Ordering
Extract and load tables in this order:
1. Small reference tables first (< 10 MB)
2. Medium tables (10 MB - 1 GB)
3. Large fact tables last (> 1 GB)

### 3. Partitioning Strategy
For tables > 10 GB:
```sql
-- Partition large tables during extraction
-- This creates multiple files for parallel loading
DECLARE @partition_count INT = 10;  -- Adjust based on table size
-- See extract_data.py for implementation
```

### 4. Monitoring Progress
```sql
-- Check migration progress
SELECT 
    phase,
    status,
    COUNT(*) as table_count,
    SUM(rows_processed) as total_rows
FROM migration.migration_log
WHERE log_timestamp > DATEADD(HOUR, -1, GETDATE())
GROUP BY phase, status
ORDER BY phase, status;
```

---

## Additional Resources

- [Datatype Mapping Guide](./DATATYPE_MAPPING.md)
- [Permissions Setup Guide](./PERMISSIONS_GUIDE.md)
- [Python Migration Scripts](./scripts/)
- [Best Practices](./BEST_PRACTICES.md)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Azure Synapse to Fabric Migration Guide](https://aka.ms/fabric-migrate-synapse-dw)

---

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review migration logs in `migration.migration_log` table
3. Consult [Microsoft Fabric documentation](https://learn.microsoft.com/fabric/)
4. Open an issue in this repository

---

**Next Steps:**
1. Review [DATATYPE_MAPPING.md](./DATATYPE_MAPPING.md) for data type compatibility
2. Review [PERMISSIONS_GUIDE.md](./PERMISSIONS_GUIDE.md) for required permissions
3. Run pre-migration checklist scripts
4. Execute migration with Python scripts
