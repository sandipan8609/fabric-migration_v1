# Data Type Mapping: Azure Synapse Dedicated Pool to Microsoft Fabric Warehouse

## Table of Contents
1. [Overview](#overview)
2. [Supported Data Types](#supported-data-types)
3. [Data Type Conversions](#data-type-conversions)
4. [Unsupported Data Types](#unsupported-data-types)
5. [Special Considerations](#special-considerations)
6. [Conversion Scripts](#conversion-scripts)
7. [Testing and Validation](#testing-and-validation)

---

## Overview

Microsoft Fabric Warehouse is built on a different engine than Azure Synapse Dedicated SQL Pool, which means there are some differences in supported data types. This guide provides a comprehensive mapping of data types and recommended conversion strategies.

### Key Differences
- ✅ **Most common data types are fully supported**
- ⚠️ **Some legacy SQL Server data types are not supported**
- ⚠️ **Spatial data types (GEOMETRY, GEOGRAPHY) are not supported**
- ⚠️ **Some advanced data types (XML, JSON as native type) have limitations**

---

## Supported Data Types

### Numeric Data Types

| Azure Synapse Dedicated Pool | Fabric Warehouse | Notes |
|------------------------------|------------------|-------|
| `BIT` | `BIT` | ✅ Fully supported |
| `TINYINT` | `TINYINT` | ✅ Fully supported |
| `SMALLINT` | `SMALLINT` | ✅ Fully supported |
| `INT` | `INT` | ✅ Fully supported |
| `BIGINT` | `BIGINT` | ✅ Fully supported |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | ✅ Fully supported (p: 1-38, s: 0-p) |
| `NUMERIC(p,s)` | `NUMERIC(p,s)` | ✅ Fully supported (same as DECIMAL) |
| `MONEY` | `DECIMAL(19,4)` | ⚠️ Convert to DECIMAL |
| `SMALLMONEY` | `DECIMAL(10,4)` | ⚠️ Convert to DECIMAL |
| `FLOAT` | `FLOAT` | ✅ Fully supported |
| `REAL` | `REAL` | ✅ Fully supported |

**Recommendations:**
- Convert `MONEY` and `SMALLMONEY` to `DECIMAL` for better precision and compatibility
- Use `DECIMAL(19,4)` for monetary values to match MONEY precision

---

### String Data Types

| Azure Synapse Dedicated Pool | Fabric Warehouse | Notes |
|------------------------------|------------------|-------|
| `CHAR(n)` | `CHAR(n)` | ✅ Fully supported (n: 1-8000) |
| `VARCHAR(n)` | `VARCHAR(n)` | ✅ Fully supported (n: 1-8000) |
| `VARCHAR(MAX)` | `VARCHAR(8000)` | ⚠️ Max length is 8000 in Fabric |
| `NCHAR(n)` | `NCHAR(n)` | ✅ Fully supported (n: 1-4000) |
| `NVARCHAR(n)` | `NVARCHAR(n)` | ✅ Fully supported (n: 1-4000) |
| `NVARCHAR(MAX)` | `NVARCHAR(4000)` | ⚠️ Max length is 4000 in Fabric |
| `TEXT` | `VARCHAR(8000)` | ❌ Not supported, convert to VARCHAR |
| `NTEXT` | `NVARCHAR(4000)` | ❌ Not supported, convert to NVARCHAR |

**Recommendations:**
- Replace `VARCHAR(MAX)` with `VARCHAR(8000)`
- Replace `NVARCHAR(MAX)` with `NVARCHAR(4000)`
- Convert `TEXT` to `VARCHAR(8000)`
- Convert `NTEXT` to `NVARCHAR(4000)`
- If data exceeds max length, consider splitting into multiple columns or using external storage

---

### Date and Time Data Types

| Azure Synapse Dedicated Pool | Fabric Warehouse | Notes |
|------------------------------|------------------|-------|
| `DATE` | `DATE` | ✅ Fully supported |
| `TIME` | `TIME` | ✅ Fully supported |
| `DATETIME` | `DATETIME2(3)` | ⚠️ Convert to DATETIME2 for better precision |
| `DATETIME2` | `DATETIME2` | ✅ Fully supported |
| `SMALLDATETIME` | `DATETIME2(0)` | ⚠️ Convert to DATETIME2 |
| `DATETIMEOFFSET` | `DATETIMEOFFSET` | ✅ Fully supported |

**Recommendations:**
- Use `DATETIME2` instead of `DATETIME` for new applications
- `DATETIME2(3)` provides millisecond precision equivalent to DATETIME
- `DATETIMEOFFSET` is recommended for timezone-aware data

---

### Binary Data Types

| Azure Synapse Dedicated Pool | Fabric Warehouse | Notes |
|------------------------------|------------------|-------|
| `BINARY(n)` | `BINARY(n)` | ✅ Fully supported (n: 1-8000) |
| `VARBINARY(n)` | `VARBINARY(n)` | ✅ Fully supported (n: 1-8000) |
| `VARBINARY(MAX)` | `VARBINARY(8000)` | ⚠️ Max length is 8000 |
| `IMAGE` | `VARBINARY(8000)` | ❌ Not supported, convert to VARBINARY |

**Recommendations:**
- Convert `IMAGE` to `VARBINARY(8000)`
- For large binary objects, consider storing in Azure Blob Storage with reference in table

---

### Other Data Types

| Azure Synapse Dedicated Pool | Fabric Warehouse | Notes |
|------------------------------|------------------|-------|
| `UNIQUEIDENTIFIER` | `UNIQUEIDENTIFIER` | ✅ Fully supported |
| `XML` | `NVARCHAR(4000)` | ❌ Not supported, store as string |
| `SQL_VARIANT` | Not supported | ❌ Convert to specific type |
| `HIERARCHYID` | Not supported | ❌ Convert to VARCHAR |
| `GEOMETRY` | Not supported | ❌ Convert to VARCHAR/WKT format |
| `GEOGRAPHY` | Not supported | ❌ Convert to VARCHAR/WKT format |
| `ROWVERSION` | `BINARY(8)` | ⚠️ Convert to BINARY(8) |
| `TIMESTAMP` | `BINARY(8)` | ⚠️ Convert to BINARY(8) |

---

## Data Type Conversions

### Conversion Priority Matrix

| Priority | Source Type | Target Type | Action Required |
|----------|-------------|-------------|-----------------|
| 🔴 Critical | TEXT, NTEXT, IMAGE | VARCHAR, NVARCHAR, VARBINARY | Must convert before migration |
| 🟡 Medium | MONEY, SMALLMONEY | DECIMAL | Recommended to convert |
| 🟡 Medium | DATETIME, SMALLDATETIME | DATETIME2 | Recommended to convert |
| 🟡 Medium | XML | NVARCHAR(4000) | Must convert if used |
| 🟢 Low | ROWVERSION, TIMESTAMP | BINARY(8) | Optional conversion |

---

### Conversion Scripts

#### 1. Identify Columns Requiring Conversion

```sql
-- Run on Azure Synapse Dedicated Pool
-- This script identifies all columns that need data type conversion

SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    c.name AS column_name,
    ty.name AS current_datatype,
    CASE c.max_length WHEN -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END AS max_length,
    CASE 
        WHEN ty.name IN ('text') THEN 'VARCHAR(8000)'
        WHEN ty.name IN ('ntext') THEN 'NVARCHAR(4000)'
        WHEN ty.name IN ('image') THEN 'VARBINARY(8000)'
        WHEN ty.name = 'money' THEN 'DECIMAL(19,4)'
        WHEN ty.name = 'smallmoney' THEN 'DECIMAL(10,4)'
        WHEN ty.name = 'datetime' THEN 'DATETIME2(3)'
        WHEN ty.name = 'smalldatetime' THEN 'DATETIME2(0)'
        WHEN ty.name = 'xml' THEN 'NVARCHAR(4000)'
        WHEN ty.name = 'sql_variant' THEN '⚠️ MANUAL REVIEW REQUIRED'
        WHEN ty.name = 'hierarchyid' THEN 'VARCHAR(4000)'
        WHEN ty.name = 'geometry' THEN 'VARCHAR(MAX) -- WKT Format'
        WHEN ty.name = 'geography' THEN 'VARCHAR(MAX) -- WKT Format'
        WHEN ty.name IN ('rowversion', 'timestamp') THEN 'BINARY(8)'
        WHEN ty.name = 'varchar' AND c.max_length = -1 THEN 'VARCHAR(8000)'
        WHEN ty.name = 'nvarchar' AND c.max_length = -1 THEN 'NVARCHAR(4000)'
        WHEN ty.name = 'varbinary' AND c.max_length = -1 THEN 'VARBINARY(8000)'
        ELSE '✅ No conversion needed'
    END AS recommended_datatype,
    CASE 
        WHEN ty.name IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 'hierarchyid', 'geometry', 'geography') 
             OR (ty.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1)
        THEN '🔴 REQUIRED'
        WHEN ty.name IN ('money', 'smallmoney', 'datetime', 'smalldatetime', 'rowversion', 'timestamp')
        THEN '🟡 RECOMMENDED'
        ELSE '✅ OPTIONAL'
    END AS conversion_priority
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND (
        ty.name IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 'hierarchyid', 
                    'geometry', 'geography', 'money', 'smallmoney', 'datetime', 
                    'smalldatetime', 'rowversion', 'timestamp')
        OR (ty.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1)
    )
ORDER BY 
    CASE conversion_priority 
        WHEN '🔴 REQUIRED' THEN 1 
        WHEN '🟡 RECOMMENDED' THEN 2 
        ELSE 3 
    END,
    s.name, t.name, c.name;
```

#### 2. Generate ALTER TABLE Statements

```sql
-- Run on Azure Synapse Dedicated Pool
-- This generates ALTER TABLE statements for data type conversions
-- ⚠️ Review and test these statements before executing on production!

SELECT 
    'ALTER TABLE [' + s.name + '].[' + t.name + '] ' +
    'ALTER COLUMN [' + c.name + '] ' +
    CASE 
        WHEN ty.name = 'text' THEN 'VARCHAR(8000)'
        WHEN ty.name = 'ntext' THEN 'NVARCHAR(4000)'
        WHEN ty.name = 'image' THEN 'VARBINARY(8000)'
        WHEN ty.name = 'money' THEN 'DECIMAL(19,4)'
        WHEN ty.name = 'smallmoney' THEN 'DECIMAL(10,4)'
        WHEN ty.name = 'datetime' THEN 'DATETIME2(3)'
        WHEN ty.name = 'smalldatetime' THEN 'DATETIME2(0)'
        WHEN ty.name = 'xml' THEN 'NVARCHAR(4000)'
        WHEN ty.name = 'hierarchyid' THEN 'VARCHAR(4000)'
        WHEN ty.name = 'rowversion' THEN 'BINARY(8)'
        WHEN ty.name = 'timestamp' THEN 'BINARY(8)'
        WHEN ty.name = 'varchar' AND c.max_length = -1 THEN 'VARCHAR(8000)'
        WHEN ty.name = 'nvarchar' AND c.max_length = -1 THEN 'NVARCHAR(4000)'
        WHEN ty.name = 'varbinary' AND c.max_length = -1 THEN 'VARBINARY(8000)'
    END +
    CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
    ';' AS alter_statement,
    '-- Priority: ' + 
    CASE 
        WHEN ty.name IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 'hierarchyid', 'geometry', 'geography') 
             OR (ty.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1)
        THEN 'REQUIRED'
        ELSE 'RECOMMENDED'
    END AS priority_comment
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    AND (
        ty.name IN ('text', 'ntext', 'image', 'xml', 'money', 'smallmoney', 
                    'datetime', 'smalldatetime', 'rowversion', 'timestamp', 'hierarchyid')
        OR (ty.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1)
    )
ORDER BY s.name, t.name, c.name;
```

#### 3. Python Script for Automated Conversion Check

```python
# check_datatypes.py
"""
Scan source database and identify columns requiring data type conversion
"""
import pyodbc
import pandas as pd
from typing import Dict, List

def check_unsupported_datatypes(server: str, database: str) -> pd.DataFrame:
    """
    Check for unsupported data types in source database
    Returns a DataFrame with columns requiring conversion
    """
    
    query = """
    SELECT 
        s.name AS schema_name,
        t.name AS table_name,
        c.name AS column_name,
        ty.name AS current_datatype,
        CASE c.max_length WHEN -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END AS max_length,
        c.precision,
        c.scale,
        CASE 
            WHEN ty.name IN ('text') THEN 'VARCHAR(8000)'
            WHEN ty.name IN ('ntext') THEN 'NVARCHAR(4000)'
            WHEN ty.name IN ('image') THEN 'VARBINARY(8000)'
            WHEN ty.name = 'money' THEN 'DECIMAL(19,4)'
            WHEN ty.name = 'smallmoney' THEN 'DECIMAL(10,4)'
            WHEN ty.name = 'datetime' THEN 'DATETIME2(3)'
            WHEN ty.name = 'smalldatetime' THEN 'DATETIME2(0)'
            WHEN ty.name = 'xml' THEN 'NVARCHAR(4000)'
            WHEN ty.name = 'hierarchyid' THEN 'VARCHAR(4000)'
            WHEN ty.name = 'geometry' THEN 'VARCHAR(MAX)'
            WHEN ty.name = 'geography' THEN 'VARCHAR(MAX)'
            WHEN ty.name IN ('rowversion', 'timestamp') THEN 'BINARY(8)'
            WHEN ty.name = 'varchar' AND c.max_length = -1 THEN 'VARCHAR(8000)'
            WHEN ty.name = 'nvarchar' AND c.max_length = -1 THEN 'NVARCHAR(4000)'
            WHEN ty.name = 'varbinary' AND c.max_length = -1 THEN 'VARBINARY(8000)'
            ELSE NULL
        END AS fabric_datatype,
        CASE 
            WHEN ty.name IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 
                             'hierarchyid', 'geometry', 'geography') 
                 OR (ty.name IN ('varchar', 'nvarchar', 'varbinary') AND c.max_length = -1)
            THEN 'REQUIRED'
            WHEN ty.name IN ('money', 'smallmoney', 'datetime', 'smalldatetime', 
                             'rowversion', 'timestamp')
            THEN 'RECOMMENDED'
            ELSE 'OK'
        END AS conversion_priority
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY conversion_priority, s.name, t.name, c.name
    """
    
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Authentication=ActiveDirectoryInteractive"
        conn = pyodbc.connect(conn_str)
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"\n{'='*80}")
        print(f"Data Type Conversion Analysis")
        print(f"{'='*80}\n")
        
        # Filter only columns needing conversion
        df_conv = df[df['fabric_datatype'].notna()]
        
        if len(df_conv) == 0:
            print("✅ No data type conversions required!")
            return df_conv
        
        # Summary by priority
        summary = df_conv.groupby('conversion_priority').size()
        print("Summary:")
        for priority, count in summary.items():
            icon = "🔴" if priority == "REQUIRED" else "🟡"
            print(f"  {icon} {priority}: {count} columns")
        
        print(f"\n{'-'*80}")
        print("Detailed List:")
        print(f"{'-'*80}\n")
        
        for _, row in df_conv.iterrows():
            icon = "🔴" if row['conversion_priority'] == "REQUIRED" else "🟡"
            print(f"{icon} [{row['schema_name']}].[{row['table_name']}].[{row['column_name']}]")
            print(f"   Current:     {row['current_datatype']}")
            print(f"   Recommended: {row['fabric_datatype']}")
            print()
        
        return df_conv
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python check_datatypes.py <server> <database>")
        print("Example: python check_datatypes.py mysynapse.sql.azuresynapse.net mydb")
        sys.exit(1)
    
    server = sys.argv[1]
    database = sys.argv[2]
    
    df = check_unsupported_datatypes(server, database)
    
    # Export to CSV
    if len(df) > 0:
        output_file = f"datatype_conversion_{database}.csv"
        df.to_csv(output_file, index=False)
        print(f"\nResults exported to: {output_file}")
```

---

## Unsupported Data Types

### Complete List of Unsupported Types

The following data types are **NOT supported** in Microsoft Fabric Warehouse:

| Data Type | Recommended Alternative | Migration Strategy |
|-----------|------------------------|-------------------|
| `TEXT` | `VARCHAR(8000)` | Direct conversion, check for truncation |
| `NTEXT` | `NVARCHAR(4000)` | Direct conversion, check for truncation |
| `IMAGE` | `VARBINARY(8000)` | Direct conversion or store in Blob Storage |
| `XML` | `NVARCHAR(4000)` | Cast to string, may need custom parsing |
| `SQL_VARIANT` | Specific type | Analyze actual data and convert to appropriate type |
| `HIERARCHYID` | `VARCHAR(4000)` | Convert using `.ToString()` method |
| `GEOMETRY` | `VARCHAR(MAX)` | Convert to WKT (Well-Known Text) format |
| `GEOGRAPHY` | `VARCHAR(MAX)` | Convert to WKT (Well-Known Text) format |
| `ROWVERSION` | `BINARY(8)` or remove | Consider if versioning is needed |
| `TIMESTAMP` | `BINARY(8)` or remove | Consider if versioning is needed |

---

## Special Considerations

### 1. Handling VARCHAR(MAX) / NVARCHAR(MAX)

**Problem:** Fabric Warehouse has maximum lengths (VARCHAR: 8000, NVARCHAR: 4000)

**Solutions:**

#### Option A: Truncate (if data fits)
```sql
-- Check actual max length in data
SELECT 
    MAX(LEN(your_column)) as max_actual_length,
    'VARCHAR(' + CAST(MAX(LEN(your_column)) AS VARCHAR) + ')' as recommended_type
FROM your_schema.your_table;

-- If max_actual_length <= 8000, safe to convert
ALTER TABLE your_schema.your_table 
ALTER COLUMN your_column VARCHAR(8000);
```

#### Option B: Split into multiple columns
```sql
-- For very long text, split into chunks
ALTER TABLE your_schema.your_table 
ADD column_part1 VARCHAR(8000),
    column_part2 VARCHAR(8000),
    column_part3 VARCHAR(8000);

-- Populate parts
UPDATE your_schema.your_table
SET 
    column_part1 = SUBSTRING(original_column, 1, 8000),
    column_part2 = SUBSTRING(original_column, 8001, 8000),
    column_part3 = SUBSTRING(original_column, 16001, 8000);
```

#### Option C: Store in external storage
```python
# Store large text in Azure Blob Storage
# Keep reference in database
import azure.storage.blob as blob

def store_large_text(text_content, blob_name):
    # Upload to blob storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(text_content)
    
    # Return reference URL
    return blob_client.url
```

---

### 2. Handling Spatial Data (GEOMETRY/GEOGRAPHY)

**Problem:** Spatial data types not supported in Fabric Warehouse

**Solution:** Convert to WKT (Well-Known Text) format

```sql
-- Convert GEOMETRY to VARCHAR
ALTER TABLE your_schema.spatial_table
ADD geometry_wkt VARCHAR(MAX);

UPDATE your_schema.spatial_table
SET geometry_wkt = geometry_column.STAsText();

-- After migration, you can reconstruct if needed
-- Or use external tools for spatial operations
```

---

### 3. Handling XML Data

**Problem:** XML data type not supported

**Solution:** Convert to NVARCHAR

```sql
-- Convert XML to NVARCHAR
ALTER TABLE your_schema.xml_table
ADD xml_data_text NVARCHAR(4000);

UPDATE your_schema.xml_table
SET xml_data_text = CAST(xml_column AS NVARCHAR(4000));

-- ⚠️ Warning: If XML is larger than 4000 characters, it will be truncated
-- Consider storing in Blob Storage for large XML documents
```

---

### 4. Handling MONEY Data Type

**Problem:** MONEY has different precision than DECIMAL

**Solution:** Convert with exact precision

```sql
-- Check for values that might lose precision
SELECT 
    money_column,
    CAST(money_column AS DECIMAL(19,4)) as decimal_value,
    CASE 
        WHEN money_column = CAST(money_column AS DECIMAL(19,4)) THEN '✅ Safe'
        ELSE '⚠️ Precision loss'
    END as status
FROM your_schema.your_table
WHERE money_column IS NOT NULL;

-- Convert
ALTER TABLE your_schema.your_table
ALTER COLUMN money_column DECIMAL(19,4);
```

---

## Testing and Validation

### 1. Pre-Conversion Testing

```sql
-- Test conversion on a copy of the table first
SELECT TOP 1000
    original_column,
    CAST(original_column AS new_datatype) as converted_value,
    CASE 
        WHEN CAST(original_column AS NVARCHAR(MAX)) = CAST(CAST(original_column AS new_datatype) AS NVARCHAR(MAX))
        THEN '✅ OK'
        ELSE '❌ Data loss'
    END as validation
FROM your_schema.your_table;
```

### 2. Post-Migration Validation

```python
# validate_datatypes.py
"""
Validate data types after migration
"""
import pyodbc
import pandas as pd

def validate_datatypes(source_server, source_db, target_workspace, target_db):
    """
    Compare data types between source and target
    """
    
    query = """
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
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    
    # Get source datatypes
    source_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={source_server};DATABASE={source_db};Authentication=ActiveDirectoryInteractive"
    )
    source_df = pd.read_sql(query, source_conn)
    source_conn.close()
    
    # Get target datatypes
    target_server = f"{target_workspace}.datawarehouse.fabric.microsoft.com"
    target_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={target_server};DATABASE={target_db};Authentication=ActiveDirectoryInteractive"
    )
    target_df = pd.read_sql(query, target_conn)
    target_conn.close()
    
    # Compare
    comparison = source_df.merge(
        target_df,
        on=['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME'],
        suffixes=('_source', '_target'),
        how='outer'
    )
    
    # Find mismatches
    mismatches = comparison[
        (comparison['DATA_TYPE_source'] != comparison['DATA_TYPE_target']) |
        (comparison['CHARACTER_MAXIMUM_LENGTH_source'] != comparison['CHARACTER_MAXIMUM_LENGTH_target'])
    ]
    
    if len(mismatches) == 0:
        print("✅ All data types match!")
    else:
        print(f"⚠️ Found {len(mismatches)} datatype mismatches:")
        print(mismatches[['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 
                         'DATA_TYPE_source', 'DATA_TYPE_target']])
    
    return mismatches

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 5:
        print("Usage: python validate_datatypes.py <source_server> <source_db> <target_workspace> <target_db>")
        sys.exit(1)
    
    validate_datatypes(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
```

---

## Summary Checklist

Before migration, ensure:

- [ ] Identified all columns with unsupported data types
- [ ] Generated ALTER TABLE scripts for required conversions
- [ ] Tested conversions on a subset of data
- [ ] Verified no data truncation occurs
- [ ] Documented any manual conversions needed (SQL_VARIANT, GEOMETRY, etc.)
- [ ] Updated application code to handle new data types
- [ ] Planned for handling VARCHAR(MAX) / NVARCHAR(MAX) if data exceeds limits
- [ ] Validated conversion scripts in non-production environment

---

## Additional Resources

- [Microsoft Fabric Data Types Documentation](https://learn.microsoft.com/fabric/data-warehouse/data-types)
- [SQL Server to Fabric Migration Guide](https://aka.ms/fabric-migrate-synapse-dw)
- [Main Migration Guide](./MIGRATION_GUIDE.md)
- [Permissions Guide](./PERMISSIONS_GUIDE.md)

---

**Next Steps:**
1. Run `check_datatypes.py` on your source database
2. Review and execute generated ALTER TABLE statements
3. Validate conversions before proceeding with migration
4. Proceed to [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)
