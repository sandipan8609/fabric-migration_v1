"""
Test script to validate migration_helpers.py module imports and basic functionality.

This script can be run to verify that the helper functions are properly structured
and can be imported without errors.
"""

import sys
import os

# Add the utils directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

def test_imports():
    """Test that all classes and functions can be imported"""
    print("Testing imports from migration_helpers...")
    
    try:
        from migration_helpers import ConnectionHelper, MigrationUtils, StorageHelper, Colors
        print("✓ Successfully imported ConnectionHelper")
        print("✓ Successfully imported MigrationUtils")
        print("✓ Successfully imported StorageHelper")
        print("✓ Successfully imported Colors")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_color_codes():
    """Test color code functionality"""
    print("\nTesting color codes...")
    
    try:
        from migration_helpers import Colors
        print(f"{Colors.GREEN}✓ Green color code works{Colors.END}")
        print(f"{Colors.RED}✓ Red color code works{Colors.END}")
        print(f"{Colors.YELLOW}✓ Yellow color code works{Colors.END}")
        print(f"{Colors.BLUE}✓ Blue color code works{Colors.END}")
        print(f"{Colors.BOLD}✓ Bold text works{Colors.END}")
        return True
    except Exception as e:
        print(f"✗ Color test failed: {e}")
        return False

def test_connection_helper_methods():
    """Test that ConnectionHelper methods exist"""
    print("\nTesting ConnectionHelper methods...")
    
    try:
        from migration_helpers import ConnectionHelper
        
        # Check methods exist
        assert hasattr(ConnectionHelper, 'connect_azure_sql'), "Missing connect_azure_sql method"
        print("✓ ConnectionHelper.connect_azure_sql exists")
        
        assert hasattr(ConnectionHelper, 'connect_fabric_warehouse'), "Missing connect_fabric_warehouse method"
        print("✓ ConnectionHelper.connect_fabric_warehouse exists")
        
        assert hasattr(ConnectionHelper, 'get_spark_token'), "Missing get_spark_token method"
        print("✓ ConnectionHelper.get_spark_token exists")
        
        return True
    except Exception as e:
        print(f"✗ ConnectionHelper test failed: {e}")
        return False

def test_migration_utils_methods():
    """Test that MigrationUtils methods exist"""
    print("\nTesting MigrationUtils methods...")
    
    try:
        from migration_helpers import MigrationUtils
        
        # Check methods exist
        assert hasattr(MigrationUtils, 'setup_external_objects'), "Missing setup_external_objects method"
        print("✓ MigrationUtils.setup_external_objects exists")
        
        assert hasattr(MigrationUtils, 'get_tables_list'), "Missing get_tables_list method"
        print("✓ MigrationUtils.get_tables_list exists")
        
        assert hasattr(MigrationUtils, 'log_operation'), "Missing log_operation method"
        print("✓ MigrationUtils.log_operation exists")
        
        assert hasattr(MigrationUtils, 'validate_row_count'), "Missing validate_row_count method"
        print("✓ MigrationUtils.validate_row_count exists")
        
        return True
    except Exception as e:
        print(f"✗ MigrationUtils test failed: {e}")
        return False

def test_storage_helper_methods():
    """Test that StorageHelper methods exist"""
    print("\nTesting StorageHelper methods...")
    
    try:
        from migration_helpers import StorageHelper
        
        # Check methods exist
        assert hasattr(StorageHelper, 'get_adls_path'), "Missing get_adls_path method"
        print("✓ StorageHelper.get_adls_path exists")
        
        assert hasattr(StorageHelper, 'read_parquet_with_spark'), "Missing read_parquet_with_spark method"
        print("✓ StorageHelper.read_parquet_with_spark exists")
        
        assert hasattr(StorageHelper, 'write_parquet_with_spark'), "Missing write_parquet_with_spark method"
        print("✓ StorageHelper.write_parquet_with_spark exists")
        
        return True
    except Exception as e:
        print(f"✗ StorageHelper test failed: {e}")
        return False

def test_adls_path_construction():
    """Test ADLS path construction"""
    print("\nTesting ADLS path construction...")
    
    try:
        from migration_helpers import StorageHelper
        
        path = StorageHelper.get_adls_path("mystorageaccount", "mycontainer")
        expected = "abfss://mycontainer@mystorageaccount.dfs.core.windows.net"
        assert path == expected, f"Path mismatch: {path} != {expected}"
        print(f"✓ Basic path construction works: {path}")
        
        path_with_folder = StorageHelper.get_adls_path("mystorageaccount", "mycontainer", "schema/table")
        expected_with_folder = "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/schema/table"
        assert path_with_folder == expected_with_folder, f"Path mismatch: {path_with_folder} != {expected_with_folder}"
        print(f"✓ Path with folder works: {path_with_folder}")
        
        return True
    except Exception as e:
        print(f"✗ ADLS path test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("="*70)
    print("Migration Helpers Validation Tests")
    print("="*70)
    
    tests = [
        test_imports,
        test_color_codes,
        test_connection_helper_methods,
        test_migration_utils_methods,
        test_storage_helper_methods,
        test_adls_path_construction
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\n" + "="*70)
    print("Test Summary")
    print("="*70)
    print(f"Total tests: {len(results)}")
    print(f"Passed: {sum(results)}")
    print(f"Failed: {len(results) - sum(results)}")
    
    if all(results):
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    exit(main())
