# Permissions Guide: Azure Dedicated Pool to Fabric Warehouse Migration

## Table of Contents
1. [Overview](#overview)
2. [Azure Dedicated SQL Pool Permissions](#azure-dedicated-sql-pool-permissions)
3. [Azure Storage Account Permissions](#azure-storage-account-permissions)
4. [Microsoft Fabric Warehouse Permissions](#microsoft-fabric-warehouse-permissions)
5. [Service Principal Setup](#service-principal-setup)
6. [Managed Identity Setup](#managed-identity-setup)
7. [Validation Scripts](#validation-scripts)
8. [Troubleshooting](#troubleshooting)

---

## Overview

This guide provides comprehensive information about all permissions required to successfully execute a migration from Azure Synapse Dedicated SQL Pool to Microsoft Fabric Warehouse.

### Permission Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    PERMISSION LAYERS                         │
└─────────────────────────────────────────────────────────────┘

1. SOURCE (Azure Dedicated SQL Pool)
   ├── Database Reader Permissions
   ├── Schema Access
   ├── Bulk Operations (for CETAS)
   └── External Data Source Management

2. STORAGE (Azure Data Lake Storage Gen2)
   ├── Storage Blob Data Contributor (Write access)
   ├── Storage Blob Data Reader (Read access)
   └── Container-level permissions

3. TARGET (Microsoft Fabric Warehouse)
   ├── Workspace Contributor/Admin
   ├── Database Writer Permissions
   ├── Schema Creation Rights
   └── Bulk Load Permissions
```

---

## Azure Dedicated SQL Pool Permissions

### Required Permissions for Migration User

The user or service principal executing the migration needs the following permissions on the **source** Azure Dedicated SQL Pool:

#### 1. Database-Level Permissions

```sql
-- Run as database administrator on Azure Dedicated SQL Pool

-- Grant database reader access
ALTER ROLE db_datareader ADD MEMBER [migration_user];

-- Grant permission to view metadata
GRANT VIEW DEFINITION TO [migration_user];

-- Grant permission for CETAS operations
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [migration_user];

-- Grant permission to create and manage external objects
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [migration_user];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [migration_user];

-- Grant permission to create database scoped credentials
GRANT ALTER ANY DATABASE SCOPED CREDENTIAL TO [migration_user];
```

#### 2. Schema-Level Permissions

```sql
-- Grant access to all user schemas
-- Replace 'dbo' with your schema names

GRANT SELECT ON SCHEMA::dbo TO [migration_user];
GRANT SELECT ON SCHEMA::sales TO [migration_user];
GRANT SELECT ON SCHEMA::staging TO [migration_user];

-- Or grant select on all schemas dynamically
DECLARE @sql NVARCHAR(MAX) = N'';

SELECT @sql = @sql + 
    'GRANT SELECT ON SCHEMA::' + QUOTENAME(name) + ' TO [migration_user];' + CHAR(13)
FROM sys.schemas
WHERE name NOT IN ('sys', 'INFORMATION_SCHEMA', 'guest', 'db_owner', 
                   'db_accessadmin', 'db_securityadmin', 'db_ddladmin',
                   'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader',
                   'db_denydatawriter');

EXEC sp_executesql @sql;
```

#### 3. Permissions for Managed Identity (if using)

If using Synapse Workspace Managed Identity:

```sql
-- Create user for managed identity
-- Replace 'mysynapseworkspace' with your workspace name
CREATE USER [mysynapseworkspace] FROM EXTERNAL PROVIDER;

-- Grant required permissions
ALTER ROLE db_datareader ADD MEMBER [mysynapseworkspace];
GRANT VIEW DEFINITION TO [mysynapseworkspace];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [mysynapseworkspace];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [mysynapseworkspace];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [mysynapseworkspace];
GRANT ALTER ANY DATABASE SCOPED CREDENTIAL TO [mysynapseworkspace];
```

#### 4. Permissions for Service Principal (if using)

```sql
-- Create user for service principal
-- Replace with your service principal application ID
CREATE USER [migration_sp] FROM EXTERNAL PROVIDER WITH OBJECT_ID = '<app-id>';

-- Grant required permissions
ALTER ROLE db_datareader ADD MEMBER [migration_sp];
GRANT VIEW DEFINITION TO [migration_sp];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [migration_sp];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [migration_sp];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [migration_sp];
GRANT ALTER ANY DATABASE SCOPED CREDENTIAL TO [migration_sp];
```

### Permission Validation Script

```sql
-- Verify permissions for migration user
-- Run as the migration user to verify access

-- Check database-level permissions
SELECT 
    permission_name,
    state_desc
FROM sys.database_permissions
WHERE grantee_principal_id = USER_ID()
    AND permission_name IN (
        'ADMINISTER DATABASE BULK OPERATIONS',
        'ALTER ANY EXTERNAL DATA SOURCE',
        'ALTER ANY EXTERNAL FILE FORMAT',
        'ALTER ANY DATABASE SCOPED CREDENTIAL',
        'VIEW DEFINITION'
    );

-- Check role memberships
SELECT 
    r.name as role_name
FROM sys.database_role_members drm
INNER JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
WHERE drm.member_principal_id = USER_ID();

-- Test read access to a sample table
SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA');
```

---

## Azure Storage Account Permissions

### Required Permissions for Storage Account

The migration process requires read/write access to Azure Data Lake Storage Gen2 for staging data.

#### 1. Azure RBAC Roles

Assign the following roles at the **Storage Account** or **Container** level:

##### Option A: Using Service Principal or Managed Identity

```bash
#!/bin/bash
# assign_storage_permissions.sh

# Variables
STORAGE_ACCOUNT_NAME="mystorageaccount"
CONTAINER_NAME="migration-staging"
PRINCIPAL_ID="<service-principal-or-managed-identity-object-id>"
RESOURCE_GROUP="migration-rg"

# Get storage account resource ID
STORAGE_ID=$(az storage account show \
    --name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --query id -o tsv)

# Assign Storage Blob Data Contributor role (read/write access)
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee $PRINCIPAL_ID \
    --scope "$STORAGE_ID"

echo "✅ Storage Blob Data Contributor role assigned"

# Verify assignment
az role assignment list \
    --assignee $PRINCIPAL_ID \
    --scope "$STORAGE_ID" \
    --query "[].{Role:roleDefinitionName, Scope:scope}" \
    --output table
```

##### Option B: Using Access Keys

```bash
#!/bin/bash
# get_storage_key.sh

STORAGE_ACCOUNT_NAME="mystorageaccount"
RESOURCE_GROUP="migration-rg"

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
    --account-name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --query '[0].value' -o tsv)

echo "Storage Account Key: $STORAGE_KEY"
echo "⚠️ Store this securely - do not commit to source control"
```

##### Option C: Using SAS Token

```bash
#!/bin/bash
# generate_sas_token.sh

STORAGE_ACCOUNT_NAME="mystorageaccount"
CONTAINER_NAME="migration-staging"
RESOURCE_GROUP="migration-rg"

# Calculate expiry date (30 days from now)
EXPIRY_DATE=$(date -u -d "30 days" '+%Y-%m-%dT%H:%MZ')

# Generate SAS token with read/write/list permissions
SAS_TOKEN=$(az storage container generate-sas \
    --account-name $STORAGE_ACCOUNT_NAME \
    --name $CONTAINER_NAME \
    --permissions rwl \
    --expiry $EXPIRY_DATE \
    --auth-mode key \
    --as-user \
    --output tsv)

echo "SAS Token: ?$SAS_TOKEN"
echo "Expires: $EXPIRY_DATE"
echo "⚠️ Store this securely - do not commit to source control"
```

#### 2. Required RBAC Roles

| Role | Purpose | Scope | Required For |
|------|---------|-------|--------------|
| **Storage Blob Data Contributor** | Read/write/delete blobs | Storage Account or Container | ✅ Recommended (most secure) |
| **Storage Blob Data Reader** | Read blobs only | Storage Account or Container | Fabric Warehouse (read-only) |
| **Storage Blob Data Owner** | Full control | Storage Account | Only if role assignment needed |

#### 3. Network Access Configuration

```bash
#!/bin/bash
# configure_storage_network.sh

STORAGE_ACCOUNT_NAME="mystorageaccount"
RESOURCE_GROUP="migration-rg"

# Allow Azure services to access storage account
az storage account update \
    --name $STORAGE_ACCOUNT_NAME \
    --resource-group $RESOURCE_GROUP \
    --default-action Allow

# Or add specific IP ranges
# az storage account network-rule add \
#     --account-name $STORAGE_ACCOUNT_NAME \
#     --resource-group $RESOURCE_GROUP \
#     --ip-address <your-ip-address>

echo "✅ Network access configured"
```

### Creating Database Scoped Credential in Synapse

```sql
-- Run on Azure Dedicated SQL Pool

-- Option 1: Using Managed Identity (Recommended)
CREATE DATABASE SCOPED CREDENTIAL MigrationCredential
WITH IDENTITY = 'Managed Identity';
GO

-- Option 2: Using Service Principal
CREATE DATABASE SCOPED CREDENTIAL MigrationCredential
WITH 
    IDENTITY = '<client-id>@https://login.microsoftonline.com/<tenant-id>/oauth2/token',
    SECRET = '<client-secret>';
GO

-- Option 3: Using SAS Token
CREATE DATABASE SCOPED CREDENTIAL MigrationCredential
WITH 
    IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '<sas-token-without-leading-question-mark>';
GO

-- Option 4: Using Storage Account Key
CREATE DATABASE SCOPED CREDENTIAL MigrationCredential
WITH 
    IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '<storage-account-key>';
GO

-- Create external data source
CREATE EXTERNAL DATA SOURCE MigrationStaging
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://migration-staging@mystorageaccount.dfs.core.windows.net',
    CREDENTIAL = MigrationCredential
);
GO

-- Test access
-- This will fail if permissions are not correct
SELECT * FROM sys.external_data_sources WHERE name = 'MigrationStaging';
```

---

## Microsoft Fabric Warehouse Permissions

### Required Permissions for Fabric Warehouse

#### 1. Workspace-Level Permissions

The user or service principal needs appropriate access to the Fabric workspace:

| Role | Permissions | Required For |
|------|-------------|--------------|
| **Admin** | Full control over workspace | ✅ Creating warehouse, managing all resources |
| **Member** | Can create and edit items | ✅ Creating tables, loading data |
| **Contributor** | Can edit existing items | Modifying existing warehouse |
| **Viewer** | Read-only access | ❌ Not sufficient for migration |

##### Assign Workspace Role via Portal

1. Navigate to Microsoft Fabric workspace
2. Click on **Workspace settings** → **Manage access**
3. Add user/service principal with **Admin** or **Member** role

##### Assign Workspace Role via PowerShell

```powershell
# Note: This requires PowerShell (included for completeness)
# For Python/Bash alternatives, use the Fabric REST API

# Install required module
# Install-Module -Name MicrosoftPowerBIMgmt

# Connect to Fabric
# Connect-PowerBIServiceAccount

# Assign workspace access
# Add-PowerBIWorkspaceUser -Id <workspace-id> -UserPrincipalName user@contoso.com -AccessRight Admin
```

#### 2. Warehouse-Level Permissions

Once in the warehouse, grant appropriate SQL permissions:

```sql
-- Run as Warehouse administrator in Fabric Warehouse

-- Grant full control for migration
ALTER ROLE db_owner ADD MEMBER [migration_user];

-- Or grant specific permissions
ALTER ROLE db_datareader ADD MEMBER [migration_user];
ALTER ROLE db_datawriter ADD MEMBER [migration_user];
ALTER ROLE db_ddladmin ADD MEMBER [migration_user];

-- Grant schema creation permission
GRANT CREATE SCHEMA TO [migration_user];
GRANT CREATE TABLE TO [migration_user];

-- Grant bulk operations permission (for COPY INTO)
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [migration_user];
GRANT ALTER ANY EXTERNAL DATA SOURCE TO [migration_user];
GRANT ALTER ANY EXTERNAL FILE FORMAT TO [migration_user];
```

#### 3. Schema-Level Permissions

```sql
-- Grant permissions on specific schemas
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON SCHEMA::dbo TO [migration_user];
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON SCHEMA::sales TO [migration_user];

-- Or create migration-specific schema
CREATE SCHEMA migration;
GRANT ALL ON SCHEMA::migration TO [migration_user];
```

#### 4. Service Principal Access to Fabric

To use a service principal with Fabric Warehouse:

```sql
-- Create user from service principal
-- Replace with your service principal application ID
CREATE USER [migration_sp] FROM EXTERNAL PROVIDER;

-- Grant permissions
ALTER ROLE db_owner ADD MEMBER [migration_sp];
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [migration_sp];
```

### Permission Validation Script

```sql
-- Run as migration user in Fabric Warehouse
-- Verify you have required permissions

-- Check database roles
SELECT 
    r.name as role_name,
    m.type_desc as member_type
FROM sys.database_role_members drm
INNER JOIN sys.database_principals r ON drm.role_principal_id = r.principal_id
INNER JOIN sys.database_principals m ON drm.member_principal_id = m.principal_id
WHERE m.name = USER_NAME();

-- Check specific permissions
SELECT 
    permission_name,
    state_desc
FROM sys.database_permissions
WHERE grantee_principal_id = USER_ID()
ORDER BY permission_name;

-- Test table creation
CREATE TABLE test_permissions (id INT, name VARCHAR(100));
DROP TABLE test_permissions;

-- Test bulk operations
-- This should not error if permissions are correct
SELECT * FROM sys.database_principals WHERE name = USER_NAME();
```

---

## Service Principal Setup

### Step-by-Step Service Principal Configuration

#### 1. Create Service Principal

```bash
#!/bin/bash
# create_service_principal.sh

SP_NAME="fabric-migration-sp"
SUBSCRIPTION_ID="<your-subscription-id>"

# Create service principal
echo "Creating service principal..."
SP_OUTPUT=$(az ad sp create-for-rbac \
    --name $SP_NAME \
    --role Contributor \
    --scopes /subscriptions/$SUBSCRIPTION_ID)

# Extract values
CLIENT_ID=$(echo $SP_OUTPUT | jq -r '.appId')
CLIENT_SECRET=$(echo $SP_OUTPUT | jq -r '.password')
TENANT_ID=$(echo $SP_OUTPUT | jq -r '.tenant')
OBJECT_ID=$(az ad sp show --id $CLIENT_ID --query id -o tsv)

echo ""
echo "✅ Service Principal Created"
echo "================================"
echo "Application (Client) ID: $CLIENT_ID"
echo "Object ID: $OBJECT_ID"
echo "Client Secret: $CLIENT_SECRET"
echo "Tenant ID: $TENANT_ID"
echo "================================"
echo ""
echo "⚠️ IMPORTANT: Save these values securely!"
echo ""

# Create .env file
cat > .env << EOF
# Service Principal Credentials
AZURE_TENANT_ID=$TENANT_ID
AZURE_CLIENT_ID=$CLIENT_ID
AZURE_CLIENT_SECRET=$CLIENT_SECRET
AZURE_OBJECT_ID=$OBJECT_ID
EOF

echo "Credentials saved to .env file"
echo "⚠️ Add .env to .gitignore to prevent accidental commits"
```

#### 2. Assign Permissions to Service Principal

```bash
#!/bin/bash
# assign_sp_permissions.sh

# Variables (set these)
SP_OBJECT_ID="<service-principal-object-id>"
RESOURCE_GROUP="migration-rg"
STORAGE_ACCOUNT="mystorageaccount"
SYNAPSE_WORKSPACE="mysynapseworkspace"

# Assign Storage Blob Data Contributor to storage account
echo "Assigning storage permissions..."
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee $SP_OBJECT_ID \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

# Assign Synapse Administrator role (if using Synapse workspace)
echo "Assigning Synapse permissions..."
az synapse role assignment create \
    --workspace-name $SYNAPSE_WORKSPACE \
    --role "Synapse Administrator" \
    --assignee $SP_OBJECT_ID

echo "✅ Permissions assigned"
```

#### 3. Test Service Principal Authentication

```python
#!/usr/bin/env python3
# test_service_principal.py

import os
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import pyodbc

def test_service_principal_auth():
    """Test service principal authentication to Azure resources"""
    
    # Load credentials from environment
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    
    if not all([tenant_id, client_id, client_secret]):
        print("❌ Missing credentials in environment variables")
        return False
    
    # Create credential
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    
    # Test 1: Storage Account Access
    print("\n1. Testing Storage Account access...")
    try:
        storage_account = "mystorageaccount"
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=credential
        )
        account_info = blob_service_client.get_account_information()
        print("   ✅ Storage Account access successful")
    except Exception as e:
        print(f"   ❌ Storage Account access failed: {e}")
        return False
    
    # Test 2: Synapse Database Access
    print("\n2. Testing Synapse database access...")
    try:
        server = "mysynapse.sql.azuresynapse.net"
        database = "mydatabase"
        
        # Get access token
        token = credential.get_token("https://database.windows.net/.default")
        
        # Connect to database
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database}"
        conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"   ✅ Synapse access successful")
        conn.close()
    except Exception as e:
        print(f"   ❌ Synapse access failed: {e}")
        return False
    
    # Test 3: Fabric Warehouse Access
    print("\n3. Testing Fabric Warehouse access...")
    try:
        workspace = "myworkspace"
        warehouse = "mywarehouse"
        server = f"{workspace}.datawarehouse.fabric.microsoft.com"
        
        # Get access token for Fabric
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        
        # Connect to Fabric Warehouse
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={warehouse}"
        conn = pyodbc.connect(conn_str, attrs_before={'AccessToken': token.token})
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        print(f"   ✅ Fabric Warehouse access successful (Database: {db_name})")
        conn.close()
    except Exception as e:
        print(f"   ❌ Fabric Warehouse access failed: {e}")
        return False
    
    print("\n" + "="*60)
    print("✅ All authentication tests passed!")
    print("="*60 + "\n")
    return True

if __name__ == "__main__":
    # Load .env file if exists
    from dotenv import load_dotenv
    load_dotenv()
    
    test_service_principal_auth()
```

---

## Managed Identity Setup

### Option 1: System-Assigned Managed Identity

```bash
#!/bin/bash
# enable_managed_identity.sh

# For Azure VM
VM_NAME="migration-vm"
RESOURCE_GROUP="migration-rg"

echo "Enabling system-assigned managed identity on VM..."
az vm identity assign \
    --name $VM_NAME \
    --resource-group $RESOURCE_GROUP

# Get managed identity object ID
MI_OBJECT_ID=$(az vm identity show \
    --name $VM_NAME \
    --resource-group $RESOURCE_GROUP \
    --query principalId -o tsv)

echo "Managed Identity Object ID: $MI_OBJECT_ID"

# For Synapse Workspace (if not already enabled)
SYNAPSE_WORKSPACE="mysynapseworkspace"

az synapse workspace update \
    --name $SYNAPSE_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --managed-identity-enabled true
```

### Option 2: User-Assigned Managed Identity

```bash
#!/bin/bash
# create_user_assigned_identity.sh

IDENTITY_NAME="migration-identity"
RESOURCE_GROUP="migration-rg"
LOCATION="eastus"

# Create user-assigned managed identity
echo "Creating user-assigned managed identity..."
az identity create \
    --name $IDENTITY_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION

# Get identity details
MI_OBJECT_ID=$(az identity show \
    --name $IDENTITY_NAME \
    --resource-group $RESOURCE_GROUP \
    --query principalId -o tsv)

MI_CLIENT_ID=$(az identity show \
    --name $IDENTITY_NAME \
    --resource-group $RESOURCE_GROUP \
    --query clientId -o tsv)

echo ""
echo "User-Assigned Managed Identity Created:"
echo "  Name: $IDENTITY_NAME"
echo "  Object ID: $MI_OBJECT_ID"
echo "  Client ID: $MI_CLIENT_ID"
```

### Assign Permissions to Managed Identity

```bash
#!/bin/bash
# assign_managed_identity_permissions.sh

MI_OBJECT_ID="<managed-identity-object-id>"
RESOURCE_GROUP="migration-rg"
STORAGE_ACCOUNT="mystorageaccount"
SUBSCRIPTION_ID="<your-subscription-id>"

# Assign Storage Blob Data Contributor
echo "Assigning storage permissions..."
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee $MI_OBJECT_ID \
    --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

echo "✅ Managed identity permissions assigned"
```

---

## Validation Scripts

### Complete Permission Validation Script

```bash
#!/bin/bash
# validate_all_permissions.sh

echo "========================================="
echo "Permission Validation Script"
echo "========================================="
echo ""

# Check Azure CLI authentication
echo "1. Checking Azure CLI authentication..."
if az account show &> /dev/null; then
    ACCOUNT=$(az account show --query name -o tsv)
    echo "   ✅ Authenticated as: $ACCOUNT"
else
    echo "   ❌ Not authenticated to Azure CLI"
    echo "   Run: az login"
    exit 1
fi

# Check storage account access
echo ""
echo "2. Checking storage account access..."
STORAGE_ACCOUNT="mystorageaccount"
CONTAINER="migration-staging"

if az storage container exists \
    --account-name $STORAGE_ACCOUNT \
    --name $CONTAINER \
    --auth-mode login &> /dev/null; then
    echo "   ✅ Storage account accessible"
else
    echo "   ❌ Cannot access storage account"
    echo "   Verify permissions and storage account name"
fi

# Additional checks can be added here

echo ""
echo "========================================="
echo "Validation complete"
echo "========================================="
```

### Python Permission Validation

```python
#!/usr/bin/env python3
# validate_permissions.py

"""
Comprehensive permission validation script
Tests all required permissions for migration
"""

import pyodbc
import sys
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class PermissionValidator:
    def __init__(self, config):
        self.config = config
        self.results = []
        
    def test_synapse_permissions(self):
        """Test Synapse database permissions"""
        print("\n" + "="*60)
        print("Testing Azure Synapse Permissions")
        print("="*60)
        
        try:
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={self.config['synapse_server']};"
                       f"DATABASE={self.config['synapse_database']};"
                       f"Authentication=ActiveDirectoryInteractive")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Test 1: Read access
            print("\n1. Testing read access...")
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES")
            count = cursor.fetchone()[0]
            print(f"   ✅ Read access OK ({count} tables found)")
            
            # Test 2: Bulk operations permission
            print("\n2. Testing bulk operations permission...")
            cursor.execute("""
                SELECT permission_name
                FROM sys.database_permissions
                WHERE grantee_principal_id = USER_ID()
                    AND permission_name = 'ADMINISTER DATABASE BULK OPERATIONS'
            """)
            if cursor.fetchone():
                print("   ✅ Bulk operations permission granted")
            else:
                print("   ❌ Missing ADMINISTER DATABASE BULK OPERATIONS permission")
                
            # Test 3: External data source permission
            print("\n3. Testing external data source permission...")
            cursor.execute("""
                SELECT permission_name
                FROM sys.database_permissions
                WHERE grantee_principal_id = USER_ID()
                    AND permission_name IN ('ALTER ANY EXTERNAL DATA SOURCE', 
                                           'ALTER ANY EXTERNAL FILE FORMAT')
            """)
            perms = [row[0] for row in cursor.fetchall()]
            if len(perms) >= 2:
                print("   ✅ External data source permissions granted")
            else:
                print(f"   ⚠️ Missing permissions: {2 - len(perms)}")
                
            conn.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def test_storage_permissions(self):
        """Test storage account permissions"""
        print("\n" + "="*60)
        print("Testing Storage Account Permissions")
        print("="*60)
        
        try:
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.config['storage_account']}.blob.core.windows.net",
                credential=credential
            )
            
            # Test 1: List containers
            print("\n1. Testing list containers...")
            containers = list(blob_service_client.list_containers())
            print(f"   ✅ List access OK ({len(containers)} containers)")
            
            # Test 2: Write access
            print("\n2. Testing write access...")
            container_client = blob_service_client.get_container_client(
                self.config['container']
            )
            test_blob = container_client.get_blob_client("_test_permissions.txt")
            test_blob.upload_blob("test", overwrite=True)
            print("   ✅ Write access OK")
            
            # Test 3: Delete access
            print("\n3. Testing delete access...")
            test_blob.delete_blob()
            print("   ✅ Delete access OK")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def test_fabric_permissions(self):
        """Test Fabric Warehouse permissions"""
        print("\n" + "="*60)
        print("Testing Fabric Warehouse Permissions")
        print("="*60)
        
        try:
            server = f"{self.config['fabric_workspace']}.datawarehouse.fabric.microsoft.com"
            conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                       f"SERVER={server};"
                       f"DATABASE={self.config['fabric_warehouse']};"
                       f"Authentication=ActiveDirectoryInteractive")
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Test 1: Database access
            print("\n1. Testing database access...")
            cursor.execute("SELECT DB_NAME()")
            db_name = cursor.fetchone()[0]
            print(f"   ✅ Database access OK (Connected to: {db_name})")
            
            # Test 2: Create table permission
            print("\n2. Testing create table permission...")
            try:
                cursor.execute("CREATE TABLE _test_permissions (id INT)")
                cursor.execute("DROP TABLE _test_permissions")
                print("   ✅ Create table permission OK")
            except:
                print("   ❌ Missing create table permission")
            
            # Test 3: Bulk load permission
            print("\n3. Testing bulk load permission...")
            cursor.execute("""
                SELECT permission_name
                FROM sys.database_permissions
                WHERE grantee_principal_id = USER_ID()
                    AND permission_name = 'ADMINISTER DATABASE BULK OPERATIONS'
            """)
            if cursor.fetchone():
                print("   ✅ Bulk load permission granted")
            else:
                print("   ❌ Missing ADMINISTER DATABASE BULK OPERATIONS permission")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False
    
    def run_all_tests(self):
        """Run all permission tests"""
        print("\n" + "="*60)
        print("PERMISSION VALIDATION")
        print("="*60)
        
        results = {
            'Synapse': self.test_synapse_permissions(),
            'Storage': self.test_storage_permissions(),
            'Fabric': self.test_fabric_permissions()
        }
        
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        
        all_passed = True
        for component, passed in results.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"{component}: {status}")
            if not passed:
                all_passed = False
        
        print("="*60)
        
        if all_passed:
            print("\n✅ All permission checks passed!")
            print("You are ready to proceed with migration.")
            return 0
        else:
            print("\n❌ Some permission checks failed.")
            print("Please review and fix the issues above before proceeding.")
            return 1

if __name__ == "__main__":
    # Configuration
    config = {
        'synapse_server': 'mysynapse.sql.azuresynapse.net',
        'synapse_database': 'mydatabase',
        'storage_account': 'mystorageaccount',
        'container': 'migration-staging',
        'fabric_workspace': 'myworkspace',
        'fabric_warehouse': 'mywarehouse'
    }
    
    validator = PermissionValidator(config)
    sys.exit(validator.run_all_tests())
```

---

## Troubleshooting

### Common Permission Issues

#### Issue 1: "Access Denied" when creating CETAS

**Symptoms:**
```
Msg 105019: External table creation failed. Error: Access to the remote server is denied.
```

**Solution:**
1. Verify managed identity has "Storage Blob Data Contributor" role
2. Check network firewall rules
3. Verify database scoped credential is correct

#### Issue 2: "Forbidden" when accessing storage

**Symptoms:**
```
Error: Status: 403 (Forbidden)
```

**Solution:**
1. Verify RBAC role assignment
2. Wait 5-10 minutes for role propagation
3. Check storage account network rules

#### Issue 3: Cannot create user from external provider

**Symptoms:**
```
Cannot find the object because it does not exist or you do not have permissions.
```

**Solution:**
1. Ensure service principal exists in Azure AD
2. Verify you're using the correct object ID (not application ID)
3. Check that Azure AD admin is configured for SQL server

---

## Permission Checklist

Before starting migration, verify:

### Source (Azure Dedicated SQL Pool)
- [ ] User/SP has `db_datareader` role
- [ ] User/SP has `ADMINISTER DATABASE BULK OPERATIONS` permission
- [ ] User/SP has `ALTER ANY EXTERNAL DATA SOURCE` permission
- [ ] User/SP has `ALTER ANY EXTERNAL FILE FORMAT` permission
- [ ] User/SP can read from all source schemas

### Storage (ADLS Gen2)
- [ ] User/SP has "Storage Blob Data Contributor" role
- [ ] Container exists and is accessible
- [ ] Network rules allow access from source and target
- [ ] Database scoped credential created in source database

### Target (Fabric Warehouse)
- [ ] User/SP has workspace Admin or Member role
- [ ] User/SP has `db_datawriter` role in warehouse
- [ ] User/SP has `CREATE SCHEMA` permission
- [ ] User/SP has `CREATE TABLE` permission
- [ ] User/SP has `ADMINISTER DATABASE BULK OPERATIONS` permission

### Validation
- [ ] Run permission validation scripts
- [ ] Test connectivity from migration environment
- [ ] Verify authentication works for all resources

---

## Additional Resources

- [Azure Synapse Access Control](https://learn.microsoft.com/azure/synapse-analytics/security/synapse-workspace-access-control-overview)
- [Azure Storage Authorization](https://learn.microsoft.com/azure/storage/common/authorize-data-access)
- [Fabric Workspace Roles](https://learn.microsoft.com/fabric/get-started/roles-workspaces)
- [Service Principal Best Practices](https://learn.microsoft.com/azure/active-directory/develop/security-best-practices-for-app-registration)

---

**Next Steps:**
1. Run permission validation scripts
2. Fix any permission issues identified
3. Proceed to [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)
