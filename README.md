# fabric-migration

This repository contains a collection of scripts and utilities designed to assist with the migration to Fabric, covering various workloads.

## 🚀 Quick Start: Azure Dedicated Pool to Fabric Warehouse

**New!** Complete migration guide with Python/Bash scripts (no PowerShell required):

1. **[📘 Migration Guide](MIGRATION_GUIDE.md)** - Comprehensive step-by-step guide
2. **[🔄 Data Type Mapping](DATATYPE_MAPPING.md)** - Handle datatype differences between platforms
3. **[🔐 Permissions Guide](PERMISSIONS_GUIDE.md)** - Setup all required permissions
4. **[⚡ Quick Start](QUICK_START.md)** - Get started in 15 minutes

### Migration Scripts (Python + Bash)

All migration scripts are located in the [`/scripts`](scripts/) directory:

```bash
# 1. Setup environment
cd scripts
./setup_environment.sh

# 2. Run pre-migration checks
./pre_migration_checks.sh

# 3. Extract data from Azure Dedicated Pool
python3 extract_data.py \
    --server mysynapse.sql.azuresynapse.net \
    --database mydatabase \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 6

# 4. Load data to Fabric Warehouse
python3 load_data.py \
    --workspace myworkspace \
    --warehouse mywarehouse \
    --storage-account mystorageaccount \
    --container migration-staging \
    --parallel-jobs 8 \
    --validate-rows

# 5. Validate migration
python3 validate_migration.py \
    --source-server mysynapse.sql.azuresynapse.net \
    --source-database mydatabase \
    --target-workspace myworkspace \
    --target-warehouse mywarehouse \
    --generate-report
```

See [scripts/README.md](scripts/README.md) for detailed documentation.

### Migration Notebooks (PySpark)

**New!** Interactive PySpark notebooks for running migration steps in Fabric:

All migration notebooks are located in the [`/notebooks`](notebooks/) directory:

- **[01_extract_data.ipynb](notebooks/01_extract_data.ipynb)** - Extract data from Azure Synapse to ADLS
- **[02_load_data.ipynb](notebooks/02_load_data.ipynb)** - Load data from ADLS to Fabric Warehouse
- **[03_validate_migration.ipynb](notebooks/03_validate_migration.ipynb)** - Validate migration completeness
- **[Helper Functions](notebooks/utils/migration_helpers.py)** - Shared utilities for connections and operations

See [notebooks/README.md](notebooks/README.md) for detailed documentation on running notebooks in Fabric.

---

## From Azure Synapse to Fabric

### Data Warehouse

- **[NEW] [Comprehensive Migration Guide](MIGRATION_GUIDE.md)** - Complete guide with scripts
- **[NEW] [PySpark Notebooks](notebooks/README.md)** - Interactive notebooks for Fabric
- **[NEW] [Data Type Mapping Guide](DATATYPE_MAPPING.md)** - Datatype compatibility reference
- **[NEW] [Permissions Guide](PERMISSIONS_GUIDE.md)** - Security and access setup
- [Official Microsoft documentation](https://aka.ms/fabric-migrate-synapse-dw)
- [Existing PowerShell scripts and utils](/data-warehouse)

### Data Engineering (Spark)

- [Azure Synapse Spark to Fabric migration guidance documentation](https://aka.ms/fabric-migrate-synapse-spark)
- [Scripts and utils](/data-engineering)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.