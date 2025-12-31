# Azure Dedicated Pool to Fabric Warehouse Migration - Complete Package

## Overview

This repository now contains a **complete, production-ready migration solution** for migrating from Azure Synapse Analytics Dedicated SQL Pool to Microsoft Fabric Warehouse, with special attention to:

✅ **No PowerShell** - All scripts are Python and Bash  
✅ **Datatype differences** - Comprehensive mapping and conversion guide  
✅ **Permissions** - Complete permission requirements for all systems  
✅ **Step-by-step process** - From planning to validation  

---

## 📚 Documentation (3,091 lines)

### 1. [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) (576 lines)
**Complete end-to-end migration guide**

**Covers:**
- Migration architecture with diagrams
- Pre-migration checklist with SQL queries
- Step-by-step migration process
- Post-migration validation procedures
- Troubleshooting common issues
- Performance optimization tips

**Key Sections:**
- Timeline estimates by database size
- Network and resource prerequisites
- Setup scripts for storage and service principals
- CETAS and COPY INTO implementation
- Row count and data type validation
- Statistics updates

---

### 2. [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md) (632 lines)
**Comprehensive data type compatibility guide**

**Covers:**
- Complete datatype matrix (60+ types)
- Supported, unsupported, and conversion-required types
- Conversion scripts (SQL + Python)
- Special considerations for:
  - VARCHAR(MAX) / NVARCHAR(MAX) handling
  - Spatial data (GEOMETRY/GEOGRAPHY)
  - XML and JSON data
  - MONEY and legacy types
- Testing and validation procedures

**Key Features:**
- SQL queries to identify problematic columns
- Automated ALTER TABLE statement generation
- Python script for datatype analysis
- Before/after conversion examples
- Truncation risk assessment

---

### 3. [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md) (1,079 lines)
**Complete permission setup for all systems**

**Covers:**
- **Azure Dedicated SQL Pool** permissions:
  - Database-level grants (BULK OPERATIONS, etc.)
  - Schema-level permissions
  - Managed identity setup
  - Service principal configuration
- **Azure Storage Account** permissions:
  - RBAC role assignments
  - SAS token generation
  - Network access configuration
  - Database scoped credential creation
- **Microsoft Fabric Warehouse** permissions:
  - Workspace-level roles
  - Warehouse-level grants
  - Service principal access
- Setup scripts for:
  - Service principal creation
  - Role assignments
  - Permission validation

**Key Features:**
- Copy-paste SQL scripts
- Bash automation scripts
- Python validation tool
- Troubleshooting guide
- Permission checklist

---

### 4. [QUICK_START.md](QUICK_START.md) (204 lines)
**Get started in 15 minutes**

**Covers:**
- Prerequisites checklist
- Quick setup (5 minutes)
- 4-step migration process
- Common issues and solutions
- Performance tips
- Next steps after migration

---

### 5. [scripts/README.md](scripts/README.md)
**Comprehensive script documentation**

**Covers:**
- Script overview and comparison
- Detailed usage instructions
- Authentication methods
- Environment variables
- Performance tuning guide
- Troubleshooting

---

## 🐍 Python Scripts (55KB total)

### 1. extract_data.py (17KB)
**Extract data from Azure Dedicated Pool to ADLS Gen2**

**Features:**
- Parallel extraction (configurable workers)
- Automatic table discovery with metadata
- External data source setup (master key, credential, data source)
- Parquet format with Snappy compression
- Large table partitioning (> 1GB)
- Progress tracking with colored output
- Error handling and logging
- Connection retry logic

**Usage:**
```bash
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6
```

---

### 2. load_data.py (27KB)
**Load data from ADLS Gen2 to Fabric Warehouse**

**Features:**
- Parallel loading (configurable workers)
- Automatic schema and table creation
- Table structure inference from source
- COPY INTO with retry logic (3 attempts)
- Row count validation against source
- Statistics updates post-load
- Progress tracking with colored output
- Error file management

**Usage:**
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

### 3. validate_migration.py (13KB)
**Validate migration completeness and accuracy**

**Features:**
- Row count comparison (source vs target)
- Missing table detection
- Extra table detection
- Validation report generation
- Colored output for easy reading
- Mismatch percentage calculation

**Usage:**
```bash
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

---

## 🔧 Bash Scripts (9KB total)

### 1. setup_environment.sh (3.6KB)
**Automated environment setup**

**Features:**
- Python version check (3.8+)
- pip package installation
- ODBC driver verification
- Azure CLI check
- .env template creation
- Authentication test

**Usage:**
```bash
cd scripts
./setup_environment.sh
```

---

### 2. pre_migration_checks.sh (5.6KB)
**Pre-migration validation suite**

**Features:**
- Configuration validation
- Azure authentication check
- Storage account access test
- Source database connectivity
- Target warehouse connectivity
- Permission verification
- Embedded Python tests

**Usage:**
```bash
cd scripts
./pre_migration_checks.sh
```

---

## 📦 Additional Files

### requirements.txt
Python dependencies:
- azure-identity
- azure-storage-blob
- pyodbc
- pandas
- python-dotenv
- colorama

### .env (template)
Configuration template for:
- Azure credentials (optional)
- Source connection details
- Storage account settings
- Target warehouse details
- Migration parameters

---

## 🚀 Migration Workflow

### Quick Overview

```
1. Setup (10 min)
   └─> Run setup_environment.sh
   └─> Configure .env file

2. Pre-checks (5 min)
   └─> Run pre_migration_checks.sh
   └─> Verify permissions

3. Extract (varies)
   └─> Run extract_data.py
   └─> Monitor progress
   └─> Data stored in ADLS Gen2

4. Load (varies)
   └─> Run load_data.py
   └─> Monitor progress
   └─> Automatic validation

5. Validate (5 min)
   └─> Run validate_migration.py
   └─> Review report
   └─> Verify critical tables

6. Complete
   └─> Update statistics
   └─> Test queries
   └─> Archive/cleanup
```

---

## ⏱️ Performance Estimates

### By Database Size

| Size | Extract Time | Load Time | Total Time |
|------|-------------|-----------|------------|
| < 10 GB | 10-30 min | 5-15 min | 15-45 min |
| 10-100 GB | 30 min - 2 hrs | 15 min - 1 hr | 45 min - 3 hrs |
| 100 GB - 1 TB | 2-8 hrs | 1-4 hrs | 3-12 hrs |
| > 1 TB | 8-24 hrs | 4-12 hrs | 12-36 hrs |

### Optimization Tips

**Parallel Jobs Recommendations:**
- Small (< 10 GB): Extract 2-4, Load 4-6
- Medium (10-100 GB): Extract 4-8, Load 6-10
- Large (> 100 GB): Extract 8-12, Load 10-16

---

## 🎯 Key Features

### Datatype Handling ✅
- 60+ datatype mappings documented
- Automatic conversion recommendations
- SQL scripts to identify issues
- Python validation tool
- Special handling for:
  - VARCHAR(MAX) → VARCHAR(8000)
  - NVARCHAR(MAX) → NVARCHAR(4000)
  - XML → NVARCHAR(4000)
  - MONEY → DECIMAL(19,4)
  - GEOMETRY/GEOGRAPHY → VARCHAR (WKT)

### Permissions ✅
- **Source (Azure Dedicated Pool):**
  - ADMINISTER DATABASE BULK OPERATIONS
  - ALTER ANY EXTERNAL DATA SOURCE
  - ALTER ANY EXTERNAL FILE FORMAT
  - db_datareader role

- **Storage (ADLS Gen2):**
  - Storage Blob Data Contributor
  - Container access
  - Network rules

- **Target (Fabric Warehouse):**
  - Workspace Admin or Member
  - db_datawriter role
  - CREATE SCHEMA/TABLE permissions
  - ADMINISTER DATABASE BULK OPERATIONS

### No PowerShell ✅
- All scripts in Python (3 scripts)
- Bash for automation (2 scripts)
- Cross-platform compatible
- Easy to customize

---

## 📋 Checklists

### Pre-Migration Checklist
- [ ] Read MIGRATION_GUIDE.md
- [ ] Review DATATYPE_MAPPING.md
- [ ] Setup PERMISSIONS_GUIDE.md
- [ ] Run setup_environment.sh
- [ ] Configure .env file
- [ ] Run pre_migration_checks.sh
- [ ] Verify all connections work

### During Migration Checklist
- [ ] Start with small test tables
- [ ] Monitor extraction progress
- [ ] Verify files in ADLS Gen2
- [ ] Start loading when extraction completes
- [ ] Monitor loading progress
- [ ] Check for errors in logs

### Post-Migration Checklist
- [ ] Run validate_migration.py
- [ ] Review validation report
- [ ] Spot check critical tables
- [ ] Compare row counts
- [ ] Test sample queries
- [ ] Update statistics (EXEC sp_updatestats)
- [ ] Verify performance
- [ ] Archive/delete staging files

---

## 🆘 Troubleshooting

### Connection Issues
```bash
# Check Azure login
az login
az account show

# Test ODBC driver
odbcinst -q -d

# Run connectivity tests
./pre_migration_checks.sh
```

### Permission Issues
- See [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md)
- Run validation scripts
- Check RBAC assignments
- Verify service principal roles

### Datatype Issues
- See [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md)
- Run datatype analysis scripts
- Review conversion recommendations
- Test with sample tables

### Performance Issues
- Adjust parallel jobs count
- Check network bandwidth
- Verify Fabric capacity units
- Consider table partitioning

---

## 📖 Documentation Index

| Document | Purpose | Size |
|----------|---------|------|
| [README.md](README.md) | Main repository overview | Updated |
| [QUICK_START.md](QUICK_START.md) | 15-minute quick start | 204 lines |
| [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) | Complete migration guide | 576 lines |
| [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md) | Datatype compatibility | 632 lines |
| [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md) | Permission setup | 1,079 lines |
| [scripts/README.md](scripts/README.md) | Script documentation | Detailed |

---

## 🎓 Getting Started

### For First-Time Users
1. Start with [QUICK_START.md](QUICK_START.md)
2. Review [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) introduction
3. Setup environment with `setup_environment.sh`
4. Run `pre_migration_checks.sh`
5. Start migration with small tables

### For Experienced Users
1. Review [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md) for compatibility
2. Setup [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md) requirements
3. Configure `.env` file
4. Run extract → load → validate scripts
5. Monitor and optimize

### For DBAs
1. Read complete [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)
2. Study [PERMISSIONS_GUIDE.md](PERMISSIONS_GUIDE.md)
3. Review [DATATYPE_MAPPING.md](DATATYPE_MAPPING.md)
4. Plan migration windows
5. Prepare rollback procedures

---

## ✅ What's Included

**Documentation:**
- ✅ 5 comprehensive guides (3,091 lines)
- ✅ Architecture diagrams
- ✅ SQL query examples
- ✅ Troubleshooting guides
- ✅ Performance tuning tips

**Scripts:**
- ✅ 3 Python scripts (production-ready)
- ✅ 2 Bash automation scripts
- ✅ requirements.txt
- ✅ .env template

**Features:**
- ✅ No PowerShell required
- ✅ Complete datatype handling
- ✅ Detailed permissions guide
- ✅ Parallel processing
- ✅ Error handling
- ✅ Validation suite
- ✅ Colored output
- ✅ Progress tracking

**Support:**
- ✅ Quick start guide
- ✅ Troubleshooting sections
- ✅ Example commands
- ✅ Validation scripts
- ✅ Best practices

---

## 🎉 You're Ready!

This package provides everything needed for a successful migration from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse.

**Start here:** [QUICK_START.md](QUICK_START.md)

**Questions?** Check the troubleshooting sections in each guide.

**Good luck with your migration!** 🚀
