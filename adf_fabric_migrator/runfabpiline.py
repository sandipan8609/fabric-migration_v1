
# -*- coding: utf-8 -*-
import re
import time
from typing import Dict, Optional, Tuple, Any
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

GUID_RE = re.compile(r"^[0-9a-fA-F\-]{36}$")
DEFAULT_TIMEOUT = (10, 60)  # (connect, read) seconds

class FabricPipelineError(RuntimeError):
    def __init__(self, msg: str, http_status: Optional[int] = None, payload: Optional[Dict[str, Any]] = None):
        super().__init__(msg)
        self.http_status = http_status
        self.payload = payload or {}

def _validate_guid(name: str, guid: str) -> None:
    if not guid or not GUID_RE.match(guid):
        raise ValueError(f"{name} appears invalid: '{guid}' (expect GUID)")

def _http_post(url: str, headers: Dict[str, str], json: Dict[str, Any],
               timeout: Tuple[int, int] = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    try:
        resp = requests.post(url, headers=headers, json=json, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise FabricPipelineError(f"HTTP POST failed for {url}: {e}") from e
    if not (200 <= resp.status_code < 300):
        raise FabricPipelineError(
            f"POST {url} returned {resp.status_code}: {resp.text}",
            http_status=resp.status_code,
            payload=resp.json() if 'application/json' in resp.headers.get('Content-Type', '') else {}
        )
    return resp.json()

def _http_get(url: str, headers: Dict[str, str],
              timeout: Tuple[int, int] = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise FabricPipelineError(f"HTTP GET failed for {url}: {e}") from e
    if not (200 <= resp.status_code < 300):
        raise FabricPipelineError(
            f"GET {url} returned {resp.status_code}: {resp.text}",
            http_status=resp.status_code,
            payload=resp.json() if 'application/json' in resp.headers.get('Content-Type', '') else {}
        )
    return resp.json()

def get_pipeline_config(spark: SparkSession, entity_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves pipeline & payload config from control tables, with minimal scan.
    """
    q = f"""
    SELECT g.entity_id, g.source_table, g.staging_table_name, g.pipeline_id,
           p.pipeline_url, p.expected_parameters
    FROM lh_config.etl_gold_copy_control g
    JOIN lh_config.etl_pipeline_mapping p
      ON g.pipeline_id = p.pipeline_id
    WHERE g.entity_id = '{entity_id}'
      AND g.is_active = TRUE
      AND p.is_active = TRUE
    """
    df: DataFrame = spark.sql(q).limit(1)  # avoid full scan
    rows = df.collect()
    return rows[0].asDict() if rows else None

def validate_config(config: Dict[str, Any]) -> None:
    # Basic fields
    required = ["entity_id", "source_table", "staging_table_name", "pipeline_id", "pipeline_url", "expected_parameters"]
    missing = [k for k in required if k not in config or config[k] is None]
    if missing:
        raise ValueError(f"Control config missing required fields: {missing}")
    _validate_guid("pipeline_id", str(config["pipeline_id"]))

    # Parameter sanity: expected_parameters is assumed to be a comma‑sep list or JSON array
    expected = config["expected_parameters"]
    if isinstance(expected, str):
        exp = [x.strip() for x in expected.split(",") if x.strip()]
    elif isinstance(expected, list):
        exp = [str(x).strip() for x in expected if str(x).strip()]
    else:
        exp = []

    provided = {"entity_id", "source", "target"}  # what create_payload will include
    missing_params = set(exp) - provided
    if missing_params:
        # If your pipeline expects more, fail early
        raise ValueError(f"Pipeline expects parameters {sorted(exp)}, but provided {sorted(provided)}; "
                         f"missing {sorted(missing_params)}. Update create_payload() or control table.")

def get_fabric_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """
    Client‑credentials token. If MSAL is available in your environment,
    consider using it (comment block below).
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://api.fabric.microsoft.com/.default",
    }
    try:
        resp = requests.post(url, data=payload, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise FabricPipelineError(f"Token acquisition failed: {e}")
    token = resp.json().get("access_token")
    if not token:
        raise FabricPipelineError("Token response did not include 'access_token'", http_status=resp.status_code, payload=resp.json())
    return token

# If you can install MSAL, use this (it handles retries/caching better):
# from msal import ConfidentialClientApplication
# def get_fabric_access_token_msal(tenant_id: str, client_id: str, client_secret: str) -> str:
#     app = ConfidentialClientApplication(
#         client_id=client_id,
#         authority=f"https://login.microsoftonline.com/{tenant_id}",
#         client_credential=client_secret,
#     )
#     result = app.acquire_token_silent(scopes=["https://api.fabric.microsoft.com/.default"], account=None)
#     if not result:
#         result = app.acquire_token_for_client(scopes=["https://api.fabric.microsoft.com/.default"])
#     if "access_token" not in result:
#         raise FabricPipelineError(f"MSAL token error: {result.get('error_description', result)}")
#     return result["access_token"]

def create_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Constructs payload for Fabric pipeline trigger.
    Adjust to match expected parameters defined in your pipeline.
    """
    return {
        "executionData": {
            "parameters": {
                "entity_id": config["entity_id"],
                "source": config["source_table"],
                "target": config["staging_table_name"]
            }
        }
    }

def trigger_pipeline(workspace_id: str, pipeline_id: str, token: str, payload: Dict[str, Any]) -> str:
    """
    Starts a pipeline run. Returns run instance ID.
    """
    _validate_guid("workspace_id", workspace_id)
    _validate_guid("pipeline_id", pipeline_id)

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    data = _http_post(url, headers, payload)
    run_id = data.get("id") or data.get("jobInstanceId")  # API has used "id"
    if not run_id:
        raise FabricPipelineError(f"Trigger response missing run id: {data}")
    return run_id

def poll_pipeline_status(workspace_id: str, pipeline_id: str, run_id: str, token: str,
                         max_wait_secs: int = 600, first_delay: int = 5, backoff: float = 1.5) -> Dict[str, Any]:
    """
    Polls status with exponential backoff until terminal state or timeout.
    Returns a dict with 'status' and 'details' for diagnostics.
    """
    _validate_guid("workspace_id", workspace_id)
    _validate_guid("pipeline_id", pipeline_id)

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{run_id}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    waited = 0
    delay = first_delay
    last = {}

    while waited <= max_wait_secs:
        try:
            last = _http_get(url, headers)
        except FabricPipelineError as e:
            # transient errors: continue unless timeout exceeded
            last = {"status": "UNKNOWN", "error": str(e), "http_status": e.http_status}
        status = (last.get("status") or "").upper()
        print(f"[Poll +{waited:>4}s] status={status}")

        if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break

        time.sleep(delay)
        waited += delay
        delay = int(delay * backoff)

    if not last:
        last = {"status": "TIMEOUT"}

    # Try to enrich diagnostics
    details = {
        "run_id": run_id,
        "status": last.get("status"),
        "startedBy": last.get("startedBy"),
        "startTime": last.get("startTime"),
        "endTime": last.get("endTime"),
        "error": last.get("error") or last.get("message") or last.get("failureMessage"),
        "raw": last
    }
    return details

def trigger_fabric_pipeline(
        spark: SparkSession,
        entity_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        workspace_id: str) -> Dict[str, Any]:
    """
    Full orchestration: lookup config -> token -> trigger -> poll.
    Returns a dict with 'status', 'run_id', 'entity_id', and 'details'.
    """
    config = get_pipeline_config(spark, entity_id)
    if not config:
        raise ValueError(f"No active config found for entity_id='{entity_id}'")

    validate_config(config)
    pipeline_id = str(config["pipeline_id"])
    payload = create_payload(config)
    token = get_fabric_access_token(tenant_id, client_id, client_secret)
    # token = get_fabric_access_token_msal(tenant_id, client_id, client_secret)  # if MSAL available

    try:
        run_id = trigger_pipeline(workspace_id, pipeline_id, token, payload)
        result = poll_pipeline_status(workspace_id, pipeline_id, run_id, token)
        status = (result.get("status") or "").upper()
        print(f"[{entity_id}] Pipeline final status: {status}")
        return {"entity_id": entity_id, "run_id": run_id, "status": status, "details": result}
    except FabricPipelineError as ex:
        print(f"[{entity_id}] Pipeline error: {ex}")
        return {"entity_id": entity_id, "run_id": None, "status": "FAILED", "details": {"error": str(ex), "http_status": ex.http_status}}
    except Exception as ex:
        print(f"[{entity_id}] Unexpected error: {ex}")
        return {"entity_id": entity_id, "run_id": None, "status": "FAILED", "details": {"error": str(ex)}}
