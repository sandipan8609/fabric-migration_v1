#!/usr/bin/env python3
"""
Azure Fabric Capacity Management Script

Manage Microsoft Fabric capacity operations:  suspend, resume, and scale. 

Usage Examples:
    # Suspend capacity
    python3 manage_fabric_capacity.py \\
        /subscriptions/12345678-1234-1234-1234-123a12b12d1c/resourceGroups/fabric-rg/providers/Microsoft.Fabric/capacities/myf2capacity \\
        suspend

    # Resume capacity
    python3 manage_fabric_capacity. py \\
        /subscriptions/12345678-1234-1234-1234-123a12b12d1c/resourceGroups/fabric-rg/providers/Microsoft. Fabric/capacities/myf2capacity \\
        resume

    # Scale capacity
    python3 manage_fabric_capacity. py \\
        /subscriptions/12345678-1234-1234-1234-123a12b12d1c/resourceGroups/fabric-rg/providers/Microsoft. Fabric/capacities/myf2capacity \\
        scale F4
"""

import argparse
import logging
import os
import sys
import re
from typing import Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
AZURE_MANAGEMENT_SCOPE = "https://management.azure.com/"
API_VERSION = "2022-07-01-preview"
VALID_SKUS = [f"F{2**i}" for i in range(1, 12)]  # F2, F4, F8, ...  F2048

# Resource ID pattern for validation
RESOURCE_ID_PATTERN = re.compile(
    r'^/subscriptions/[a-f0-9-]+/resourceGroups/[^/]+/providers/Microsoft\.Fabric/capacities/[^/]+$',
    re. IGNORECASE
)


class FabricCapacityError(Exception):
    """Custom exception for Fabric capacity operations."""
    pass


class CapacityAlreadyInStateError(FabricCapacityError):
    """Exception raised when capacity is already in the desired state."""
    pass


def validate_resource_id(resource_id:  str) -> bool:
    """
    Validate the Azure resource ID format. 
    
    Args: 
        resource_id: The Azure resource ID to validate
        
    Returns: 
        True if valid, raises ValueError otherwise
    """
    if not RESOURCE_ID_PATTERN.match(resource_id):
        raise ValueError(
            f"Invalid resource ID format: {resource_id}\n"
            "Expected format: /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Fabric/capacities/<name>"
        )
    return True


def get_access_token() -> str:
    """
    Retrieve Azure access token using managed identity or DefaultAzureCredential.
    
    Returns:
        Access token string
        
    Raises: 
        FabricCapacityError: If token acquisition fails
    """
    identity_endpoint = os. getenv('IDENTITY_ENDPOINT')
    
    if identity_endpoint:
        # Using managed identity (e.g., in Azure Functions, App Service)
        logger.debug("Using managed identity for authentication")
        return _get_token_from_managed_identity(identity_endpoint)
    else:
        # Using DefaultAzureCredential (local dev, service principal, etc.)
        logger.debug("Using DefaultAzureCredential for authentication")
        return _get_token_from_default_credential()


def _get_token_from_managed_identity(identity_endpoint: str) -> str:
    """
    Get access token using Azure Managed Identity.
    
    Args: 
        identity_endpoint:  The managed identity endpoint URL
        
    Returns:
        Access token string
    """
    identity_header = os. getenv('IDENTITY_HEADER')
    if not identity_header:
        raise FabricCapacityError(
            "IDENTITY_HEADER environment variable not set.  "
            "Required when using managed identity."
        )
    
    url = f"{identity_endpoint}?resource={AZURE_MANAGEMENT_SCOPE}"
    headers = {
        'X-IDENTITY-HEADER':  identity_header,
        'Metadata':  'True'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        raise FabricCapacityError(f"Failed to acquire token via managed identity: {e}")
    except KeyError: 
        raise FabricCapacityError("Token response did not contain 'access_token'")


def _get_token_from_default_credential() -> str:
    """
    Get access token using Azure DefaultAzureCredential.
    
    Returns:
        Access token string
    """
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError: 
        raise FabricCapacityError(
            "azure-identity package not installed.  "
            "Install with: pip install azure-identity"
        )
    
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token(AZURE_MANAGEMENT_SCOPE)
        return token.token
    except Exception as e:
        raise FabricCapacityError(f"Failed to acquire token via DefaultAzureCredential: {e}")


def get_auth_headers(token: str) -> dict:
    """
    Build authorization headers for Azure API requests.
    
    Args: 
        token: Access token
        
    Returns: 
        Headers dictionary
    """
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }


def suspend_capacity(resource_id: str, token: str) -> dict:
    """
    Suspend a Fabric capacity.
    
    Args: 
        resource_id:  The Azure resource ID of the capacity
        token: Access token for authentication
        
    Returns:
        Response data or status information
    """
    return _execute_capacity_action(resource_id, "suspend", token)


def resume_capacity(resource_id: str, token: str) -> dict:
    """
    Resume a Fabric capacity. 
    
    Args: 
        resource_id: The Azure resource ID of the capacity
        token: Access token for authentication
        
    Returns:
        Response data or status information
    """
    return _execute_capacity_action(resource_id, "resume", token)


def _execute_capacity_action(resource_id:  str, action: str, token: str) -> dict:
    """
    Execute a capacity action (suspend/resume).
    
    Args:
        resource_id: The Azure resource ID of the capacity
        action: The action to perform ('suspend' or 'resume')
        token: Access token for authentication
        
    Returns:
        Response data or status information
        
    Raises: 
        CapacityAlreadyInStateError:  If capacity is already in desired state
        FabricCapacityError: If the operation fails
    """
    url = f"https://management.azure.com{resource_id}/{action}?api-version={API_VERSION}"
    logger.info(f"Executing {action} on capacity:  {resource_id}")
    
    try:
        response = requests.post(
            url,
            headers=get_auth_headers(token),
            timeout=60
        )
        
        if response.ok:
            logger. info(f"Successfully initiated {action} operation")
            return {"status": "success", "action": action}
        
        # Handle specific error cases
        error_info = _parse_error_response(response)
        
        if error_info. get("message") == "Service is not ready to be updated":
            logger.warning(
                f"Capacity is not ready to be updated.  "
                f"It may already be in the desired state: {action}"
            )
            raise CapacityAlreadyInStateError(
                f"Capacity may already be in state: {action}"
            )
        
        response.raise_for_status()
        
    except requests.exceptions. Timeout:
        raise FabricCapacityError(f"Request timed out while trying to {action} capacity")
    except requests.exceptions.RequestException as e:
        raise FabricCapacityError(f"Failed to {action} capacity: {e}")
    
    return {"status": "unknown", "action": action}


def scale_capacity(resource_id: str, sku: str, token: str) -> dict:
    """
    Scale a Fabric capacity to a different SKU.
    
    Args: 
        resource_id:  The Azure resource ID of the capacity
        sku: The target SKU (e.g., 'F4', 'F8')
        token: Access token for authentication
        
    Returns: 
        Response data or status information
        
    Raises: 
        ValueError: If SKU is invalid
        FabricCapacityError:  If the operation fails
    """
    if sku not in VALID_SKUS:
        raise ValueError(f"Invalid SKU: {sku}. Valid options: {', '.join(VALID_SKUS)}")
    
    url = f"https://management.azure.com{resource_id}?api-version={API_VERSION}"
    logger.info(f"Scaling capacity to {sku}:  {resource_id}")
    
    payload = {
        "sku": {
            "name":  sku,
            "tier": "Fabric"
        }
    }
    
    try: 
        response = requests. patch(
            url,
            headers=get_auth_headers(token),
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        logger.info(f"Successfully initiated scale operation to {sku}")
        return {"status":  "success", "action": "scale", "sku": sku}
        
    except requests. exceptions.Timeout:
        raise FabricCapacityError(f"Request timed out while trying to scale capacity to {sku}")
    except requests.exceptions.HTTPError as e: 
        error_info = _parse_error_response(response)
        raise FabricCapacityError(
            f"Failed to scale capacity to {sku}: {error_info.get('message', str(e))}"
        )
    except requests.exceptions.RequestException as e: 
        raise FabricCapacityError(f"Failed to scale capacity:  {e}")


def _parse_error_response(response: requests.Response) -> dict:
    """
    Safely parse error information from an API response.
    
    Args:
        response: The requests Response object
        
    Returns:
        Dictionary with error details, or empty dict if parsing fails
    """
    try:
        error_data = response.json()
        if "error" in error_data:
            return error_data["error"]
        return error_data
    except (ValueError, KeyError):
        return {"message": response.text or "Unknown error"}


def parse_arguments() -> argparse. Namespace:
    """
    Parse command-line arguments. 
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse. ArgumentParser(
        description="Manage Microsoft Fabric capacity operations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples: 
  Suspend:   %(prog)s <resource_id> suspend
  Resume:   %(prog)s <resource_id> resume
  Scale:    %(prog)s <resource_id> scale F4
        """
    )
    
    parser. add_argument(
        "resource_id",
        help="The Azure resource ID of the Fabric capacity"
    )
    
    parser.add_argument(
        "operation",
        choices=["suspend", "resume", "scale"],
        help="The operation to perform"
    )
    
    parser.add_argument(
        "sku",
        choices=VALID_SKUS,
        nargs="?",
        help="The target SKU for scale operation (e.g., F4, F8, F64)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose/debug logging"
    )
    
    args = parser.parse_args()
    
    # Validate:  SKU is required for scale operation
    if args.operation == "scale" and not args.sku:
        parser.error("The 'scale' operation requires a SKU argument (e.g., F4)")
    
    return args


def main() -> int:
    """
    Main entry point for the script.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_arguments()
    
    # Set log level based on verbosity
    if args.verbose:
        logging. getLogger().setLevel(logging.DEBUG)
    
    try:
        # Validate resource ID
        validate_resource_id(args.resource_id)
        
        # Get access token
        logger.debug("Acquiring access token...")
        token = get_access_token()
        logger.debug("Access token acquired successfully")
        
        # Execute the requested operation
        if args.operation == "suspend":
            result = suspend_capacity(args.resource_id, token)
        elif args.operation == "resume":
            result = resume_capacity(args.resource_id, token)
        elif args.operation == "scale": 
            result = scale_capacity(args.resource_id, args. sku, token)
        else: 
            # Should never reach here due to argparse choices
            raise ValueError(f"Unknown operation: {args. operation}")
        
        logger.info(f"Operation completed: {result}")
        return 0
        
    except CapacityAlreadyInStateError as e:
        logger.warning(str(e))
        return 0  # Not really an error, just a warning
        
    except (ValueError, FabricCapacityError) as e:
        logger.error(str(e))
        return 1
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 130
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__": 
    sys.exit(main())