#!/bin/bash
# setup_environment.sh
# 
# Setup the migration environment including:
# - Python dependencies
# - ODBC driver verification
# - Azure CLI login
# - Environment variables

set -e

echo "=========================================="
echo "Migration Environment Setup"
echo "=========================================="
echo ""

# Check Python version
echo "1. Checking Python version..."
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "   Python version: $PYTHON_VERSION"

REQUIRED_VERSION="3.8"
if [ "$(printf '%s\n' "$REQUIRED_VERSION" "$PYTHON_VERSION" | sort -V | head -n1)" = "$REQUIRED_VERSION" ]; then 
    echo "   ✅ Python version OK"
else
    echo "   ❌ Python 3.8 or higher required"
    exit 1
fi
echo ""

# Check pip
echo "2. Checking pip..."
if command -v pip3 &> /dev/null; then
    echo "   ✅ pip3 is installed"
else
    echo "   ❌ pip3 not found"
    exit 1
fi
echo ""

# Install Python requirements
echo "3. Installing Python dependencies..."
pip3 install -r requirements.txt --quiet
echo "   ✅ Dependencies installed"
echo ""

# Check ODBC Driver
echo "4. Checking ODBC Driver for SQL Server..."
if odbcinst -q -d | grep -q "ODBC Driver 17 for SQL Server"; then
    echo "   ✅ ODBC Driver 17 found"
elif odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    echo "   ✅ ODBC Driver 18 found"
else
    echo "   ❌ ODBC Driver for SQL Server not found"
    echo "   Install from: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
    exit 1
fi
echo ""

# Check Azure CLI
echo "5. Checking Azure CLI..."
if command -v az &> /dev/null; then
    AZ_VERSION=$(az version --query '"azure-cli"' -o tsv 2>/dev/null)
    echo "   Azure CLI version: $AZ_VERSION"
    echo "   ✅ Azure CLI is installed"
else
    echo "   ⚠️  Azure CLI not found (optional for managed identity)"
fi
echo ""

# Create .env file template if it doesn't exist
echo "6. Checking environment configuration..."
if [ ! -f .env ]; then
    echo "   Creating .env template..."
    cat > .env << 'EOF'
# Azure Credentials (optional - only if using service principal)
# Leave blank to use default Azure credential (managed identity or az login)
AZURE_TENANT_ID=
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=

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
EOF
    echo "   ✅ Created .env template"
    echo "   ⚠️  Please edit .env file with your configuration"
else
    echo "   ✅ .env file already exists"
fi
echo ""

# Test Azure authentication
echo "7. Testing Azure authentication..."
if az account show &> /dev/null; then
    ACCOUNT=$(az account show --query name -o tsv)
    echo "   ✅ Authenticated as: $ACCOUNT"
else
    echo "   ⚠️  Not authenticated to Azure"
    echo "   Run 'az login' to authenticate"
fi
echo ""

echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Edit .env file with your configuration"
echo "2. Run pre-migration validation:"
echo "   bash pre_migration_checks.sh"
echo "3. Extract data from source:"
echo "   python3 extract_data.py --server \$SOURCE_SERVER --database \$SOURCE_DATABASE ..."
echo "4. Load data to target:"
echo "   python3 load_data.py --workspace \$TARGET_WORKSPACE --warehouse \$TARGET_WAREHOUSE ..."
echo ""
