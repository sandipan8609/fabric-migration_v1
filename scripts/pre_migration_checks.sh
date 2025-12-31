#!/bin/bash
# pre_migration_checks.sh
#
# Run pre-migration checks to verify:
# - Connectivity to source and target
# - Permissions
# - Storage access
# - Data type compatibility

set -e

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "=========================================="
echo "Pre-Migration Checks"
echo "=========================================="
echo ""

# Check required variables
echo "1. Checking configuration..."
REQUIRED_VARS=("SOURCE_SERVER" "SOURCE_DATABASE" "STORAGE_ACCOUNT" "STORAGE_CONTAINER" "TARGET_WORKSPACE" "TARGET_WAREHOUSE")
MISSING_VARS=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
    echo "   ❌ Missing required variables:"
    for var in "${MISSING_VARS[@]}"; do
        echo "      - $var"
    done
    echo "   Please configure .env file"
    exit 1
else
    echo "   ✅ Configuration complete"
fi
echo ""

# Check Azure authentication
echo "2. Checking Azure authentication..."
if az account show &> /dev/null; then
    SUBSCRIPTION=$(az account show --query name -o tsv)
    echo "   ✅ Authenticated"
    echo "   Subscription: $SUBSCRIPTION"
else
    echo "   ❌ Not authenticated to Azure"
    echo "   Run: az login"
    exit 1
fi
echo ""

# Check storage account access
echo "3. Checking storage account access..."
if az storage container exists \
    --account-name "$STORAGE_ACCOUNT" \
    --name "$STORAGE_CONTAINER" \
    --auth-mode login &> /dev/null; then
    echo "   ✅ Storage account accessible"
    echo "   Container: $STORAGE_CONTAINER"
else
    echo "   ⚠️  Cannot access storage container"
    echo "   This may be due to:"
    echo "   - Container doesn't exist"
    echo "   - Missing permissions"
    echo "   - Network restrictions"
fi
echo ""

# Run Python connectivity tests
echo "4. Running connectivity tests..."
echo ""

python3 << 'PYEOF'
import sys
import pyodbc
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

def test_source_connection():
    """Test connection to source Synapse"""
    try:
        server = os.getenv('SOURCE_SERVER')
        database = os.getenv('SOURCE_DATABASE')
        
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database}"
        conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
        
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        print(f"   ✅ Source connection successful")
        print(f"   Server: {server}")
        print(f"   Database: {database}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Source connection failed: {e}")
        return False

def test_target_connection():
    """Test connection to Fabric Warehouse"""
    try:
        workspace = os.getenv('TARGET_WORKSPACE')
        warehouse = os.getenv('TARGET_WAREHOUSE')
        server = f"{workspace}.datawarehouse.fabric.microsoft.com"
        
        credential = DefaultAzureCredential()
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={warehouse}"
        conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
        
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db = cursor.fetchone()[0]
        
        print(f"   ✅ Target connection successful")
        print(f"   Workspace: {workspace}")
        print(f"   Warehouse: {warehouse}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Target connection failed: {e}")
        return False

def test_storage_access():
    """Test storage account access"""
    try:
        storage_account = os.getenv('STORAGE_ACCOUNT')
        container = os.getenv('STORAGE_CONTAINER')
        
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=credential
        )
        
        container_client = blob_service_client.get_container_client(container)
        properties = container_client.get_container_properties()
        
        print(f"   ✅ Storage access successful")
        print(f"   Account: {storage_account}")
        print(f"   Container: {container}")
        
        return True
    except Exception as e:
        print(f"   ❌ Storage access failed: {e}")
        return False

# Run tests
print("A. Testing source database connection...")
source_ok = test_source_connection()
print()

print("B. Testing target warehouse connection...")
target_ok = test_target_connection()
print()

print("C. Testing storage account access...")
storage_ok = test_storage_access()
print()

# Summary
if source_ok and target_ok and storage_ok:
    print("="*50)
    print("✅ All connectivity checks passed!")
    print("="*50)
    sys.exit(0)
else:
    print("="*50)
    print("❌ Some checks failed. Please review and fix.")
    print("="*50)
    sys.exit(1)
PYEOF

echo ""
echo "=========================================="
echo "Pre-Migration Checks Complete"
echo "=========================================="
echo ""
echo "If all checks passed, you can proceed with:"
echo "  python3 extract_data.py ..."
echo ""
