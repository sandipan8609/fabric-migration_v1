import json
import uuid
import argparse
import sys
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, List, Optional
import traceback

# ==========================================
# 1. CONFIGURATION
# ==========================================
CONFIG = {
    "workspace_id": "95e132cd-cf5f-4e15-a9e1-7506994aa23c",
    "notebook_id": "your_fabric_notebook_id",

    # Connections (GUIDs from your tenant)
    "warehouse_connection_id": "06f15094-5415-40ca-9647-985fa72a41fe",
    "lakehouse_connection_id": "e31de1f3-905a-400e-8c21-1bfcc5c7719c",
    "oracle_connection_id": "1320ffbd-c314-4267-be68-d3e63f7ff4df",
    "fabric_connection_id": "e31de1f3-905a-400e-8c21-1bfcc5c7719c",
    "blob_connection_id": "835ec99b-46ec-4d2f-86e5-7e2ca052bf0c",

    # Artifacts
    "warehouse_artifact_id": "6068bf54-5806-44df-996b-f19fac38d18c",
    "warehouse_endpoint": "uz5qo3w55cyebj7ffmgl7aydcm-zuzodfk7z4ku5kpboudjssvchq.datawarehouse.fabric.microsoft.com",
    "lakehouse_artifact_id": "2d07daef-8c0b-454d-9a31-28faec11c440",
    "lakehouse_name": "lh_sbm_bronze",
    "placeholder_pipeline_id": "40dfe58b-19e1-47bf-bafb-2b38705dd06f",

    # Copy sink selection:  "lakehouse" | "blob" | "blobfs"
    "target_sink": "lakehouse",

    # Parameter candidates to support multiple pipeline conventions
    "param_candidates": {
        "source_container": ["containerName", "blob_container"],
        "sink_folder": ["destinationPath", "blob_path"],
        "sink_file":  ["fileName", "file_name"]
    },

    # Connection mappings for LinkedServices
    "connection_mappings": {},

    # Debug printing to stdout (pre/post/mapping snapshots)
    "debug": False
}

# ==========================================
# 2. COMPREHENSIVE TYPE MAPPINGS
# ==========================================
class CopyActivityMapper:
    """
    Comprehensive ADF Copy Activity type mappings
    Based on fabric-toolbox-main patterns
    """
    
    # Source Type Mapping (50+)
    SOURCE_TYPE_MAP = {
        # Relational Databases
        'AzureSqlSource': 'AzureSqlSource',
        'SqlServerSource': 'SqlServerSource',
        'SqlSource': 'SqlServerSource',
        'SqlDWSource': 'AzureSqlDWSource',
        'SqlMISource': 'AzureSqlMISource',
        'OracleSource': 'OracleSource',
        'MySqlSource': 'MySqlSource',
        'PostgreSqlSource': 'PostgreSqlSource',
        'PostgreSqlV2Source': 'PostgreSqlV2Source',
        'DB2Source': 'Db2Source',
        'TeradataSource': 'TeradataSource',
        'SybaseSource': 'SybaseSource',
        
        # Cloud Warehouses
        'SnowflakeSource': 'SnowflakeSource',
        'SnowflakeV2Source': 'SnowflakeV2Source',
        'AmazonRedshiftSource': 'AmazonRedshiftSource',
        'GoogleBigQuerySource': 'GoogleBigQuerySource',
        
        # File-Based
        'DelimitedTextSource': 'DelimitedTextSource',
        'ParquetSource': 'ParquetSource',
        'JsonSource': 'JsonSource',
        'XmlSource': 'XmlSource',
        'AvroSource': 'AvroSource',
        'OrcSource': 'OrcSource',
        'BinarySource': 'BinarySource',
        'ExcelSource': 'ExcelSource',
        
        # Storage
        'AzureBlobStorageSource': 'BlobSource',
        'BlobSource': 'BlobSource',
        'AzureBlobFSSource': 'AzureBlobFSSource',
        'AzureDataLakeStoreSource': 'AzureDataLakeStoreSource',
        'AzureFileStorageSource': 'AzureFileStorageSource',
        
        # NoSQL
        'CosmosDbSqlApiSource': 'CosmosDbSqlApiSource',
        'MongoDbSource': 'MongoDbSource',
        'MongoDbV2Source': 'MongoDbV2Source',
        'MongoDbAtlasSource': 'MongoDbAtlasSource',
        
        # Web/REST
        'RestSource': 'RestSource',
        'HttpSource': 'HttpSource',
        'ODataSource': 'ODataSource',
        
        # Fabric
        'DataWarehouseSource': 'DataWarehouseSource',
        'LakehouseTableSource': 'LakehouseTableSource',
        
        # Other
        'SapTableSource': 'SapTableSource',
        'SalesforceSource': 'SalesforceSource',
        'DynamicsSource': 'DynamicsSource',
        'SharePointOnlineListSource': 'SharePointOnlineListSource'
    }
    
    # Sink Type Mapping (40+)
    SINK_TYPE_MAP = {
        # Relational Databases
        'AzureSqlSink': 'AzureSqlSink',
        'SqlServerSink': 'SqlServerSink',
        'SqlSink': 'SqlServerSink',
        'SqlDWSink': 'SqlDWSink',
        'SqlMISink': 'SqlMISink',
        'OracleSink': 'OracleSink',
        'MySqlSink': 'MySqlSink',
        'PostgreSqlSink': 'PostgreSqlSink',
        'PostgreSqlV2Sink': 'PostgreSqlV2Sink',
        
        # Cloud Warehouses
        'SnowflakeSink': 'SnowflakeSink',
        'SnowflakeV2Sink':  'SnowflakeV2Sink',
        
        # File-Based
        'DelimitedTextSink': 'DelimitedTextSink',
        'ParquetSink': 'ParquetSink',
        'JsonSink':  'JsonSink',
        'AvroSink': 'AvroSink',
        'OrcSink': 'OrcSink',
        'BinarySink': 'BinarySink',
        
        # Storage
        'BlobSink': 'BlobSink',
        'AzureBlobFSSink': 'AzureBlobFSSink',
        'AzureDataLakeStoreSink': 'AzureDataLakeStoreSink',
        
        # Fabric-Specific
        'LakehouseSink': 'DelimitedTextSink',
        'DataWarehouseSink': 'DataWarehouseSink',
        
        # NoSQL
        'CosmosDbSqlApiSink': 'CosmosDbSqlApiSink',
        'MongoDbSink': 'MongoDbSink',
        'MongoDbV2Sink': 'MongoDbV2Sink',
        'MongoDbAtlasSink': 'MongoDbAtlasSink'
    }
    
    # Dataset Type Mapping (50+)
    DATASET_TYPE_MAP = {
        # SQL Tables
        'AzureSqlTable': 'AzureSqlTable',
        'SqlServerTable': 'SqlServerTable',
        'AzureSqlDWTable':  'AzureSqlDWTable',
        'OracleTable': 'OracleTable',
        'MySqlTable': 'MySqlTable',
        'PostgreSqlTable':  'PostgreSqlTable',
        'PostgreSqlV2Table': 'PostgreSqlV2Table',
        
        # File Formats
        'DelimitedText': 'DelimitedText',
        'Parquet': 'Parquet',
        'Json': 'Json',
        'Xml': 'Xml',
        'Binary': 'Binary',
        'Avro': 'Avro',
        'Orc': 'Orc',
        'Excel': 'Excel',
        
        # Storage Types
        'AzureBlob': 'AzureBlob',
        'AzureBlobFSFile': 'AzureBlobFSFile',
        'AzureDataLakeStoreFile': 'AzureDataLakeStoreFile',
        
        # Cloud Warehouses
        'SnowflakeTable': 'SnowflakeTable',
        'SnowflakeV2Table': 'SnowflakeV2Table',
        
        # Fabric-Specific
        'LakehouseTable': 'LakehouseTable',
        'DataWarehouseTable': 'DataWarehouseTable',
        
        # NoSQL
        'CosmosDbSqlApiCollection': 'CosmosDbSqlApiCollection',
        'MongoDbCollection': 'MongoDbCollection',
        'MongoDbV2Collection': 'MongoDbV2Collection'
    }
    
    # Store Settings Type Mapping
    STORE_SETTINGS_MAP = {
        'AzureBlobStorageReadSettings': 'AzureBlobStorageReadSettings',
        'AzureBlobStorageWriteSettings': 'AzureBlobStorageWriteSettings',
        'AzureBlobFSReadSettings': 'AzureBlobFSReadSettings',
        'AzureBlobFSWriteSettings': 'AzureBlobFSWriteSettings',
        'AzureDataLakeStoreReadSettings': 'AzureDataLakeStoreReadSettings',
        'AzureDataLakeStoreWriteSettings': 'AzureDataLakeStoreWriteSettings',
        'LakehouseReadSettings': 'LakehouseReadSettings',
        'LakehouseWriteSettings': 'LakehouseWriteSettings',
        'HttpReadSettings': 'HttpReadSettings',
        'SftpReadSettings': 'SftpReadSettings',
        'FileServerReadSettings': 'FileServerReadSettings'
    }
    
    # Location Type Mapping
    LOCATION_TYPE_MAP = {
        'AzureBlobStorageLocation': 'AzureBlobStorageLocation',
        'AzureBlobFSLocation': 'AzureBlobFSLocation',
        'AzureDataLakeStoreLocation': 'AzureDataLakeStoreLocation',
        'LakehouseLocation': 'LakehouseLocation',
        'HttpServerLocation': 'HttpServerLocation',
        'FileServerLocation': 'FileServerLocation',
        'SftpLocation': 'SftpLocation'
    }
    
    @classmethod
    def map_source_type(cls, adf_type: str) -> str:
        return cls.SOURCE_TYPE_MAP. get(adf_type, adf_type)
    
    @classmethod
    def map_sink_type(cls, adf_type: str) -> str:
        return cls.SINK_TYPE_MAP.get(adf_type, adf_type)
    
    @classmethod
    def map_dataset_type(cls, adf_type:  str) -> str:
        return cls.DATASET_TYPE_MAP.get(adf_type, adf_type)
    
    @classmethod
    def map_store_settings_type(cls, adf_type: str) -> str:
        return cls.STORE_SETTINGS_MAP.get(adf_type, adf_type)
    
    @classmethod
    def map_location_type(cls, adf_type: str) -> str:
        return cls. LOCATION_TYPE_MAP.get(adf_type, adf_type)

class ConnectorMapping:
    """
    General ADF to Fabric connector/activity type mapping
    """
    
    # LinkedService Type Mapping
    ADF_TO_FABRIC_CONNECTOR_TYPE = {
        'AzureSqlDatabase': 'AzureSqlDatabase',
        'SqlServer': 'SqlServer',
        'AzureSqlDW': 'AzureSynapseAnalytics',
        'AzureSynapseAnalytics': 'AzureSynapseAnalytics',
        'AzureBlobStorage': 'AzureBlobStorage',
        'AzureBlobFS': 'AzureBlobFS',
        'AzureDataLakeStore': 'AzureDataLakeStoreGen1',
        'AzureDataLakeStoreGen2': 'AzureBlobFS',
        'CosmosDb': 'AzureCosmosDb',
        'MongoDb':  'MongoDB',
        'MongoDbV2': 'MongoDB',
        'Snowflake': 'Snowflake',
        'AmazonRedshift': 'AmazonRedshift',
        'GoogleBigQuery': 'GoogleBigQuery',
        'Oracle': 'Oracle',
        'OracleServiceCloud': 'OracleServiceCloud',
        'MySql': 'MySql',
        'PostgreSql': 'PostgreSql',
        'Db2': 'Db2',
        'Teradata': 'Teradata',
        'Sybase': 'Sybase',
        'HttpServer': 'RestService',
        'RestService': 'RestService',
        'OData': 'OData',
        'WebApi': 'WebApi',
        'Odbc': 'Odbc',
        'FileServer': 'FileServer',
        'FtpServer': 'FtpServer',
        'Sftp': 'Sftp',
        'Databricks': 'Databricks',
        'FabricDataPipeline': 'FabricDataPipeline',
        'FabricWarehouse': 'DataWarehouse',
        'FabricLakehouse': 'Lakehouse'
    }
    
    # Activity Type Mapping
    ADF_TO_FABRIC_ACTIVITY_TYPE = {
        'Copy': 'Copy',
        'Lookup': 'Lookup',
        'GetMetadata': 'GetMetadata',
        'Delete': 'Delete',
        'Script': 'Script',
        'SqlServerStoredProcedure': 'SqlServerStoredProcedure',
        'ExecutePipeline': 'InvokePipeline',
        'InvokePipeline': 'InvokePipeline',
        'DatabricksNotebook': 'TridentNotebook',
        'SynapseNotebook': 'TridentNotebook',
        'Custom': 'Custom',
        'ForEach': 'ForEach',
        'IfCondition': 'IfCondition',
        'Switch': 'Switch',
        'Until': 'Until',
        'Wait': 'Wait',
        'SetVariable': 'SetVariable',
        'AppendVariable': 'AppendVariable',
        'Filter': 'Filter',
        'Validation': 'Validation',
        'WebActivity': 'WebActivity',
        'WebHook': 'WebHook',
        'Fail': 'Fail',
        'HDInsightHive': 'AzureHDInsight',
        'HDInsightPig': 'AzureHDInsight',
        'HDInsightMapReduce': 'AzureHDInsight',
        'HDInsightSpark': 'AzureHDInsight',
        'HDInsightStreaming':  'AzureHDInsight',
        'DatabricksSparkJar': 'DatabricksSparkJar',
        'DatabricksSparkPython': 'DatabricksSparkPython',
        'AzureMLBatchExecution': 'AzureMLBatchExecution',
        'AzureMLUpdateResource': 'AzureMLUpdateResource',
        'AzureMLExecutePipeline': 'AzureMLExecutePipeline'
    }
    
    @classmethod
    def map_connector_type(cls, adf_type: str) -> str:
        if not adf_type:
            return 'Generic'
        fabric_type = cls.ADF_TO_FABRIC_CONNECTOR_TYPE.get(adf_type)
        if fabric_type: 
            return fabric_type
        adf_lower = adf_type.lower()
        for adf_key, fabric_val in cls.ADF_TO_FABRIC_CONNECTOR_TYPE. items():
            if adf_key.lower() == adf_lower:
                return fabric_val
        for adf_key, fabric_val in cls.ADF_TO_FABRIC_CONNECTOR_TYPE. items():
            if adf_type in adf_key or adf_key in adf_type:
                return fabric_val
        return 'Generic'
    
    @classmethod
    def map_activity_type(cls, adf_type: str) -> str:
        if not adf_type:
            return 'Unknown'
        return cls.ADF_TO_FABRIC_ACTIVITY_TYPE.get(adf_type, adf_type)

class ParameterSubstitution:
    """Handle parameter substitution for expressions"""
    
    @staticmethod
    def substitute_dataset_params(value: Any, dataset_params: Dict) -> Any:
        """Replace @dataset().paramName with actual values"""
        if isinstance(value, str):
            if value.startswith('@dataset().'):
                param_name = value.replace('@dataset().', '').strip()
                return dataset_params.get(param_name, value)
            return value
        elif isinstance(value, dict):
            if value.get('type') == 'Expression' and 'value' in value:
                expr = value['value']
                if isinstance(expr, str) and '@dataset()' in expr:
                    for param_name, param_value in dataset_params.items():
                        if f'@dataset().{param_name}' in expr:
                            if expr.strip() == f'@dataset().{param_name}':
                                return param_value
                return value
            return {k: ParameterSubstitution.substitute_dataset_params(v, dataset_params) 
                    for k, v in value.items()}
        elif isinstance(value, list):
            return [ParameterSubstitution.substitute_dataset_params(item, dataset_params) 
                    for item in value]
        return value
    
    @staticmethod
    def create_pipeline_param_expression(param_name: str) -> Dict:
        """Create @pipeline().parameters.paramName expression"""
        return {
            "value": f"@pipeline().parameters.{param_name}",
            "type": "Expression"
        }
    
    @staticmethod
    def format_value_with_type(value: Any) -> Dict:
        """Format value with proper type detection"""
        if isinstance(value, dict) and 'type' in value:
            return value
        if isinstance(value, str) and value.strip().startswith(('@', '=')):
            return {"value": value, "type": "Expression"}
        return {"value": value, "type": "String"}

# ==========================================
# 3.  LOGGING & SUMMARY
# ==========================================
class Logger:
    def __init__(self, path=None, also_stdout=False):
        self.path = path
        self.also_stdout = also_stdout
        self.fp = None
        if self.path:
            self.fp = open(self.path, "a", encoding="utf-8")
            self._write_raw(f"\n=== Log Start: {datetime.utcnow().isoformat()}Z ===\n")

    def _write_raw(self, text: str):
        if self.fp:
            self.fp. write(text)
            self.fp.flush()
        if self.also_stdout:
            print(text, end="")

    def write_section(self, title: str, payload):
        body = json.dumps(payload, indent=2, ensure_ascii=False)
        self._write_raw(f"\n=== {title} ===\n{body}\n")

    def write_pre(self, path, act):
        self.write_section(f"PRE [{path}]", act)

    def write_post(self, path, act):
        self.write_section(f"POST [{path}]", act)

    def write_mapping(self, path, from_type, to_type, notes=None):
        payload = {"from": from_type, "to": to_type}
        if notes:
            payload["notes"] = notes
        self.write_section(f"MAPPED [{path}]", payload)

    def write_summary(self, summary_dict):
        self.write_section("SUMMARY", summary_dict)

    def close(self):
        if self.fp:
            self._write_raw(f"\n=== Log End: {datetime.utcnow().isoformat()}Z ===\n")
            self.fp.close()
            self.fp = None

class SummaryCollector:
    def __init__(self):
        self.count_adf = {}
        self.count_fabric = {}
        self.mappings = []
        self.param_use = {
            "source_container": None,
            "sink_folder": None,
            "sink_file":  None
        }
        self.sink_choice = None
        self.paths_converted = []
        self.dataset_type_mappings = {}
        self.connector_type_mappings = {}
        self.source_type_mappings = {}
        self.sink_type_mappings = {}

    def bump(self, dct, key):
        dct[key] = dct. get(key, 0) + 1

    def record_mapping(self, path, from_type, to_type):
        self.mappings.append({"path": path, "from":  from_type, "to": to_type})
        self.paths_converted.append(path)
        self.bump(self.count_adf, from_type or "Unknown")
        self.bump(self.count_fabric, to_type or "Unknown")

    def record_dataset_mapping(self, adf_type, fabric_type):
        self.dataset_type_mappings[adf_type] = fabric_type

    def record_connector_mapping(self, adf_type, fabric_type):
        self.connector_type_mappings[adf_type] = fabric_type

    def record_source_type_mapping(self, adf_type, fabric_type):
        self.source_type_mappings[adf_type] = fabric_type

    def record_sink_type_mapping(self, adf_type, fabric_type):
        self.sink_type_mappings[adf_type] = fabric_type

    def set_params(self, container, folder, file):
        self.param_use["source_container"] = container
        self.param_use["sink_folder"] = folder
        self.param_use["sink_file"] = file

    def set_sink_choice(self, sink):
        self.sink_choice = sink

    def summary(self):
        return {
            "activity_counts": {
                "ADF_input": self.count_adf,
                "Fabric_output":  self.count_fabric
            },
            "mappings": self. mappings,
            "dataset_type_mappings": self. dataset_type_mappings,
            "connector_type_mappings": self.connector_type_mappings,
            "source_type_mappings": self.source_type_mappings,
            "sink_type_mappings": self.sink_type_mappings,
            "sink_choice": self.sink_choice,
            "parameter_names_used": self.param_use,
            "converted_paths": self.paths_converted
        }

LOGGER = Logger(path=None, also_stdout=False)
SUMMARY = SummaryCollector()

# ==========================================
# 4. HELPERS
# ==========================================
def clean_val(val):
    if str(val) == "[object Object]":
        return "FIX_ME_INVALID_OBJECT"
    return val

def get_flat_value(val):
    if isinstance(val, dict):
        if "value" in val:
            return get_flat_value(val["value"])
        return json.dumps(val, ensure_ascii=False)
    if val is None:
        return ""
    if isinstance(val, str):
        val = val.strip()
    return clean_val(val)

def is_expression(val):
    return isinstance(val, str) and val.strip().startswith(("@", "="))

def expr_param(name):
    return {"value": f"@pipeline().parameters.{name}", "type":  "Expression"}

def select_param_name(pipeline_props, key):
    candidates = CONFIG["param_candidates"]. get(key, [])
    params = (pipeline_props or {}).get("parameters", {}) or {}
    for c in candidates:
        if c in params:
            return c
    return candidates[0] if candidates else None

def format_sp_param(val):
    raw = get_flat_value(val)
    if is_expression(raw):
        return {
            "value": {"value": raw, "type": "Expression"},
            "type": "String"
        }
    else:
        return {"value": raw, "type": "String"}

def format_notebook_param(val):
    raw = get_flat_value(val)
    inferred_type = "String"
    if isinstance(raw, bool):
        inferred_type = "bool"
    elif isinstance(raw, int):
        inferred_type = "int"
    return {"value": raw, "type": inferred_type}

def format_invoke_param(val):
    raw = get_flat_value(val)
    return raw

def format_generic_value(val):
    raw = get_flat_value(val)
    return {"value": raw, "type": "Expression" if is_expression(raw) else "String"}

# ==========================================
# 5. ENHANCED COPY ACTIVITY CONVERTER
# ==========================================
class EnhancedCopyConverter:
    """
    Comprehensive Copy Activity Converter
    Implements full Dataset-to-DatasetSettings transformation
    """
    
    def __init__(self):
        self.mapper = CopyActivityMapper()
        self.param_sub = ParameterSubstitution()
    
    def convert_copy_full(self, activity:  Dict, pipeline_props: Optional[Dict] = None) -> Dict:
        """
        Full Copy activity conversion with comprehensive source/sink mapping
        """
        adf_type = activity. get('type')
        fabric_type = ConnectorMapping.map_activity_type(adf_type)
        
        # Base activity structure
        fabric_activity = {
            "name": activity.get("name", "UnnamedCopy"),
            "type": fabric_type,
            "dependsOn": deepcopy(activity.get("dependsOn", [])),
            "policy": self._transform_policy(activity.get("policy", {})),
            "userProperties": deepcopy(activity.get("userProperties", []))
        }
        
        # Get typeProperties
        adf_type_props = activity.get("typeProperties", {})
        
        # Detect parameter names
        container_param = select_param_name(pipeline_props, "source_container") or "containerName"
        folder_param = select_param_name(pipeline_props, "sink_folder") or "destinationPath"
        file_param = select_param_name(pipeline_props, "sink_file") or "fileName"
        SUMMARY.set_params(container_param, folder_param, file_param)
        
        # Transform source
        fabric_source = self._transform_source_comprehensive(
            adf_type_props.get("source", {}),
            container_param,
            pipeline_props
        )
        
        # Transform sink
        fabric_sink = self._transform_sink_comprehensive(
            adf_type_props. get("sink", {}),
            folder_param,
            file_param,
            pipeline_props
        )
        
        # Build typeProperties
        fabric_activity["typeProperties"] = {
            "source": fabric_source,
            "sink": fabric_sink,
            "enableStaging": adf_type_props.get("enableStaging", False)
        }
        
        # Add optional properties
        if "translator" in adf_type_props:
            fabric_activity["typeProperties"]["translator"] = deepcopy(adf_type_props["translator"])
        
        if "enableSkipIncompatibleRow" in adf_type_props:
            fabric_activity["typeProperties"]["enableSkipIncompatibleRow"] = adf_type_props["enableSkipIncompatibleRow"]
        
        if "validateDataConsistency" in adf_type_props:
            fabric_activity["typeProperties"]["validateDataConsistency"] = adf_type_props["validateDataConsistency"]
        
        if "parallelCopies" in adf_type_props:
            fabric_activity["typeProperties"]["parallelCopies"] = adf_type_props["parallelCopies"]
        
        if "dataIntegrationUnits" in adf_type_props:
            fabric_activity["typeProperties"]["dataIntegrationUnits"] = adf_type_props["dataIntegrationUnits"]
        
        SUMMARY.record_mapping(activity. get('name'), adf_type, fabric_type)
        return fabric_activity
    
    def _transform_policy(self, adf_policy: Dict) -> Dict:
        return {
            "timeout": adf_policy.get("timeout", "0.12: 00:00"),
            "retry": adf_policy.get("retry", 0),
            "retryIntervalInSeconds": adf_policy.get("retryIntervalInSeconds", 30),
            "secureOutput": adf_policy. get("secureOutput", False),
            "secureInput": adf_policy.get("secureInput", False)
        }
    
    def _transform_source_comprehensive(
        self,
        adf_source: Dict,
        container_param: str,
        pipeline_props: Optional[Dict]
    ) -> Dict:
        """Comprehensive source transformation with full property mapping"""
        
        source_type = adf_source.get("type", "DelimitedTextSource")
        fabric_source_type = self.mapper.map_source_type(source_type)
        
        # Record mapping
        SUMMARY.record_source_type_mapping(source_type, fabric_source_type)
        
        # Build base source
        fabric_source = {"type": fabric_source_type}
        
        # === SQL Source Properties ===
        if "sqlReaderQuery" in adf_source:
            fabric_source["sqlReaderQuery"] = self. param_sub.format_value_with_type(
                adf_source["sqlReaderQuery"]
            )
        
        if "sqlReaderStoredProcedureName" in adf_source:
            fabric_source["sqlReaderStoredProcedureName"] = self.param_sub.format_value_with_type(
                adf_source["sqlReaderStoredProcedureName"]
            )
        
        if "storedProcedureParameters" in adf_source:
            fabric_source["storedProcedureParameters"] = adf_source["storedProcedureParameters"]
        
        if "queryTimeout" in adf_source: 
            fabric_source["queryTimeout"] = adf_source["queryTimeout"]
        
        if "isolationLevel" in adf_source:
            fabric_source["isolationLevel"] = adf_source["isolationLevel"]
        
        if "partitionOption" in adf_source:
            fabric_source["partitionOption"] = adf_source["partitionOption"]
        
        if "partitionSettings" in adf_source:
            fabric_source["partitionSettings"] = adf_source["partitionSettings"]
        
        # === Oracle Properties ===
        if "oracleReaderQuery" in adf_source:
            fabric_source["oracleReaderQuery"] = self.param_sub.format_value_with_type(
                adf_source["oracleReaderQuery"]
            )
        
        # === File-Based Properties ===
        if "recursive" in adf_source: 
            fabric_source["recursive"] = adf_source["recursive"]
        
        if "wildcardFileName" in adf_source:
            fabric_source["wildcardFileName"] = self.param_sub.format_value_with_type(
                adf_source["wildcardFileName"]
            )
        
        if "wildcardFolderPath" in adf_source:
            fabric_source["wildcardFolderPath"] = self.param_sub.format_value_with_type(
                adf_source["wildcardFolderPath"]
            )
        
        if "modifiedDatetimeStart" in adf_source:
            fabric_source["modifiedDatetimeStart"] = self.param_sub.format_value_with_type(
                adf_source["modifiedDatetimeStart"]
            )
        
        if "modifiedDatetimeEnd" in adf_source:
            fabric_source["modifiedDatetimeEnd"] = self.param_sub.format_value_with_type(
                adf_source["modifiedDatetimeEnd"]
            )
        
        if "maxConcurrentConnections" in adf_source: 
            fabric_source["maxConcurrentConnections"] = adf_source["maxConcurrentConnections"]
        
        if "additionalColumns" in adf_source: 
            fabric_source["additionalColumns"] = adf_source["additionalColumns"]
        
        # === Store Settings ===
        if "storeSettings" in adf_source: 
            fabric_source["storeSettings"] = self._transform_store_settings(
                adf_source["storeSettings"],
                is_source=True
            )
        
        # === Format Settings ===
        if "formatSettings" in adf_source: 
            fabric_source["formatSettings"] = self._transform_format_settings(
                adf_source["formatSettings"]
            )
        
        # === Build datasetSettings ===
        fabric_source["datasetSettings"] = self._build_source_dataset_settings(
            source_type,
            container_param
        )
        
        return fabric_source
    
    def _transform_sink_comprehensive(
        self,
        adf_sink: Dict,
        folder_param: str,
        file_param: str,
        pipeline_props: Optional[Dict]
    ) -> Dict:
        """Comprehensive sink transformation with full property mapping"""
        
        sink_type = adf_sink.get("type", "DelimitedTextSink")
        fabric_sink_type = self.mapper.map_sink_type(sink_type)
        
        # Record mapping
        SUMMARY.record_sink_type_mapping(sink_type, fabric_sink_type)
        
        # Build base sink
        fabric_sink = {"type": fabric_sink_type}
        
        # === SQL Sink Properties ===
        if "writeBehavior" in adf_sink: 
            fabric_sink["writeBehavior"] = adf_sink["writeBehavior"]
        
        if "sqlWriterStoredProcedureName" in adf_sink:
            fabric_sink["sqlWriterStoredProcedureName"] = adf_sink["sqlWriterStoredProcedureName"]
        
        if "sqlWriterTableType" in adf_sink: 
            fabric_sink["sqlWriterTableType"] = adf_sink["sqlWriterTableType"]
        
        if "storedProcedureParameters" in adf_sink:
            fabric_sink["storedProcedureParameters"] = adf_sink["storedProcedureParameters"]
        
        if "tableOption" in adf_sink: 
            fabric_sink["tableOption"] = adf_sink["tableOption"]
        
        if "preCopyScript" in adf_sink:
            fabric_sink["preCopyScript"] = self.param_sub.format_value_with_type(
                adf_sink["preCopyScript"]
            )
        
        if "writeBatchSize" in adf_sink:
            fabric_sink["writeBatchSize"] = adf_sink["writeBatchSize"]
        
        if "writeBatchTimeout" in adf_sink:
            fabric_sink["writeBatchTimeout"] = adf_sink["writeBatchTimeout"]
        
        if "maxConcurrentConnections" in adf_sink:
            fabric_sink["maxConcurrentConnections"] = adf_sink["maxConcurrentConnections"]
        
        if "upsertSettings" in adf_sink:
            fabric_sink["upsertSettings"] = adf_sink["upsertSettings"]
        
        # === Store Settings ===
        if "storeSettings" in adf_sink:
            fabric_sink["storeSettings"] = self._transform_store_settings(
                adf_sink["storeSettings"],
                is_source=False
            )
        
        # === Format Settings ===
        if "formatSettings" in adf_sink:
            fabric_sink["formatSettings"] = self._transform_format_settings(
                adf_sink["formatSettings"]
            )
        
        # === Build datasetSettings ===
        fabric_sink["datasetSettings"] = self._build_sink_dataset_settings(
            folder_param,
            file_param
        )
        
        return fabric_sink
    
    def _transform_store_settings(self, adf_store_settings: Dict, is_source: bool) -> Dict:
        """Transform storeSettings"""
        settings_type = adf_store_settings. get("type")
        fabric_settings_type = self.mapper.map_store_settings_type(settings_type)
        
        fabric_settings = {"type": fabric_settings_type}
        
        # Common properties
        common_props = [
            "recursive", "wildcardFileName", "wildcardFolderPath",
            "enablePartitionDiscovery", "partitionRootPath",
            "deleteFilesAfterCompletion", "modifiedDatetimeStart",
            "modifiedDatetimeEnd", "maxConcurrentConnections",
            "disableMetricsCollection"
        ]
        
        for prop in common_props:
            if prop in adf_store_settings:
                fabric_settings[prop] = adf_store_settings[prop]
        
        # Write-specific
        if not is_source:
            write_props = ["copyBehavior", "maxRowsPerFile", "metadata", "blockSizeInMB"]
            for prop in write_props:
                if prop in adf_store_settings: 
                    fabric_settings[prop] = adf_store_settings[prop]
        
        return fabric_settings
    
    def _transform_format_settings(self, adf_format_settings: Dict) -> Dict:
        """Transform formatSettings"""
        return deepcopy(adf_format_settings)
    
    def _build_source_dataset_settings(self, source_type: str, container_param: str) -> Dict:
        """Build source datasetSettings"""
        
        # Determine dataset type based on source type
        if "Oracle" in source_type:
            fabric_dataset_type = self.mapper.map_dataset_type("OracleTable")
            SUMMARY.record_dataset_mapping("OracleTable", fabric_dataset_type)
            return {
                "annotations": [],
                "type": fabric_dataset_type,
                "schema": [],
                "externalReferences": {"connection": CONFIG["oracle_connection_id"]}
            }
        
        # Default:  DelimitedText source
        fabric_dataset_type = self.mapper.map_dataset_type("DelimitedText")
        SUMMARY.record_dataset_mapping("DelimitedText (Source)", fabric_dataset_type)
        
        return {
            "annotations": [],
            "type": fabric_dataset_type,
            "typeProperties": {
                "location": {
                    "type": "AzureBlobStorageLocation",
                    "container": expr_param(container_param)
                },
                "columnDelimiter": ",",
                "escapeChar":  "\\",
                "firstRowAsHeader": True,
                "quoteChar": "\""
            },
            "schema": [],
            "externalReferences": {"connection": CONFIG["blob_connection_id"]}
        }
    
    def _build_sink_dataset_settings(self, folder_param: str, file_param: str) -> Dict:
        """Build sink datasetSettings based on target"""
        
        target = CONFIG. get("target_sink", "lakehouse")
        SUMMARY.set_sink_choice(target)
        
        if target == "lakehouse":
            fabric_dataset_type = self.mapper.map_dataset_type("DelimitedText")
            SUMMARY.record_dataset_mapping("DelimitedText (Sink)", fabric_dataset_type)
            
            return {
                "annotations": [],
                "type": fabric_dataset_type,
                "connectionSettings": {
                    "name": CONFIG["lakehouse_name"],
                    "properties": {
                        "annotations": [],
                        "type":  "Lakehouse",
                        "typeProperties": {
                            "workspaceId": CONFIG["workspace_id"],
                            "artifactId": CONFIG["lakehouse_artifact_id"],
                            "rootFolder": "Files"
                        },
                        "externalReferences": {"connection": CONFIG["lakehouse_connection_id"]}
                    }
                },
                "typeProperties": {
                    "location": {
                        "type":  "LakehouseLocation",
                        "folderPath": expr_param(folder_param)
                    },
                    "columnDelimiter": ",",
                    "escapeChar": "\\",
                    "firstRowAsHeader": True,
                    "quoteChar": "\""
                },
                "schema": []
            }
        
        return {}

# ==========================================
# 6. ACTIVITY CONVERTERS
# ==========================================
def convert_stored_proc(act):
    adf_type = act. get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    sp_name_raw = get_flat_value(tp_old. get("storedProcedureName", ""))

    new_act["typeProperties"] = {
        "storedProcedureName": sp_name_raw,
        "storedProcedureParameters": {}
    }
    
    for k, v in tp_old.get("storedProcedureParameters", {}).items():
        new_act["typeProperties"]["storedProcedureParameters"][k] = format_sp_param(v)

    new_act["connectionSettings"] = {
        "name": "wh_sbm_gold",
        "properties": {
            "annotations": [],
            "type": "DataWarehouse",
            "typeProperties":  {
                "endpoint": CONFIG["warehouse_endpoint"],
                "artifactId": CONFIG["warehouse_artifact_id"],
                "workspaceId": CONFIG["workspace_id"]
            },
            "externalReferences":  {"connection": CONFIG["warehouse_connection_id"]}
        }
    }
    
    SUMMARY.record_mapping(act. get('name'), adf_type, fabric_type)
    return new_act

def convert_invoke_pipeline(act):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)

    old_policy = act.get("policy", {})
    new_act["policy"] = {
        "timeout": old_policy.get("timeout", "0.12:00:00"),
        "retry": old_policy.get("retry", 0),
        "retryIntervalInSeconds":  old_policy.get("retryIntervalInSeconds", 30),
        "secureOutput": old_policy.get("secureOutput", False),
        "secureInput": old_policy.get("secureInput", False)
    }
    
    tp_old = act.get("typeProperties", {}) or {}
    new_act["typeProperties"] = {
        "waitOnCompletion": tp_old.get("waitOnCompletion", "3"),
        "operationType": "InvokeFabricPipeline",
        "pipelineId": tp_old.get("pipelineId", CONFIG["placeholder_pipeline_id"]),
        "workspaceId": CONFIG["workspace_id"],
        "parameters": {}
    }
    
    for k, v in tp_old. get("parameters", {}).items():
        new_act["typeProperties"]["parameters"][k] = format_invoke_param(v)
    
    new_act["externalReferences"] = {
        "connection": CONFIG["fabric_connection_id"]
    }
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_notebook(act):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    new_act["typeProperties"] = {
        "notebookId": tp_old.get("notebookId", CONFIG["notebook_id"]),
        "workspaceId": CONFIG["workspace_id"],
        "parameters": {}
    }
    
    base_params = tp_old.get("baseParameters", {}) or tp_old.get("parameters", {})
    for k, v in base_params. items():
        new_act["typeProperties"]["parameters"][k] = format_notebook_param(v)
    
    SUMMARY.record_mapping(act. get('name'), adf_type, fabric_type)
    return new_act

def convert_copy(act, pipeline_props=None):
    """
    Enhanced Copy converter using EnhancedCopyConverter
    """
    converter = EnhancedCopyConverter()
    return converter.convert_copy_full(act, pipeline_props)

def convert_lookup(act):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act. get("typeProperties", {}) or {}
    src_old = tp_old.get("source", {}) or {}
    
    if "sqlReaderQuery" in src_old:
        sql_expr = get_flat_value(src_old["sqlReaderQuery"])
    elif "storedProcedureName" in src_old: 
        sql_expr = f"EXEC {get_flat_value(src_old['storedProcedureName'])}"
    elif "query" in src_old: 
        sql_expr = get_flat_value(src_old["query"])
    else:
        sql_expr = ""
    
    fabric_dataset_type = CopyActivityMapper.map_dataset_type("DataWarehouseTable")
    SUMMARY.record_dataset_mapping("DataWarehouseTable", fabric_dataset_type)
    
    new_act["typeProperties"] = {
        "source": {
            "type": "DataWarehouseSource",
            "sqlReaderQuery": {
                "value": sql_expr,
                "type": "Expression" if is_expression(sql_expr) else "String"
            },
            "queryTimeout": src_old.get("queryTimeout", tp_old.get("queryTimeout", "02:00:00")),
            "partitionOption": "None"
        },
        "datasetSettings": {
            "annotations": [],
            "type": fabric_dataset_type,
            "schema": [],
            "connectionSettings": {
                "name":  "wh_sbm_gold",
                "properties": {
                    "annotations": [],
                    "type": "DataWarehouse",
                    "typeProperties": {
                        "endpoint": CONFIG["warehouse_endpoint"],
                        "artifactId": CONFIG["warehouse_artifact_id"],
                        "workspaceId":  CONFIG["workspace_id"]
                    },
                    "externalReferences": {"connection": CONFIG["warehouse_connection_id"]}
                }
            }
        }
    }
    
    if "firstRowOnly" in tp_old:
        new_act["typeProperties"]["firstRowOnly"] = tp_old["firstRowOnly"]
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_get_metadata(act, pipeline_props=None):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    container_param = select_param_name(pipeline_props, "source_container") or "containerName"
    SUMMARY.param_use["source_container"] = container_param
    
    fabric_dataset_type = CopyActivityMapper.map_dataset_type("DelimitedText")
    SUMMARY.record_dataset_mapping("DelimitedText (GetMetadata)", fabric_dataset_type)
    
    new_ds = {
        "annotations": [],
        "type": fabric_dataset_type,
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "container": expr_param(container_param)
            }
        }
    }
    
    if act.get("name") == "FileMetadata":
        new_ds["typeProperties"]["location"]["fileName"] = {
            "value": "@item().name",
            "type":  "Expression"
        }
    
    new_act["typeProperties"] = {
        "datasetSettings": new_ds,
        "fieldList": tp_old.get("fieldList", []),
        "storeSettings": {"type": "AzureBlobStorageReadSettings"},
        "formatSettings": {"type": "DelimitedTextReadSettings"}
    }
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_set_variable(act):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    new_act["typeProperties"] = {
        "variableName": tp_old.get("variableName", ""),
        "value": format_generic_value(tp_old.get("value", {}))
    }
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_for_each(act, pipeline_props=None):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    new_act["typeProperties"] = {
        "items": format_generic_value(tp_old.get("items")),
        "isSequential": tp_old.get("isSequential", True),
        "activities": convert_activity_list(
            tp_old.get("activities", []),
            pipeline_props,
            parent_path=f"{act.get('name')}.ForEach"
        )
    }
    
    SUMMARY. record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_delete(act, pipeline_props=None):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping. map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    dataset_ref = tp_old.get("dataset", {})
    dataset_name = dataset_ref.get("referenceName")
    
    fabric_dataset_type = CopyActivityMapper.map_dataset_type("DelimitedText")
    SUMMARY.record_dataset_mapping(f"Delete:{dataset_name}", fabric_dataset_type)
    
    new_act["typeProperties"] = {
        "enableLogging": tp_old.get("enableLogging", False),
        "storeSettings": {
            "type": "AzureBlobStorageReadSettings",
            "recursive": True,
            "wildcardFileName": format_generic_value(tp_old.get("wildcardFileName", ""))
        },
        "datasetSettings": {
            "annotations": [],
            "type": fabric_dataset_type,
            "typeProperties": {
                "location":  {
                    "type": "AzureBlobStorageLocation"
                }
            },
            "schema": []
        }
    }
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_script(act):
    adf_type = act.get('type')
    fabric_type = ConnectorMapping.map_activity_type(adf_type)
    
    new_act = _base_props(act, fabric_type)
    tp_old = act. get("typeProperties", {}) or {}
    
    new_act["typeProperties"] = {
        "scripts": tp_old.get("scripts", []),
        "scriptBlockExecutionTimeout": tp_old.get("scriptBlockExecutionTimeout", "02:00:00")
    }
    
    new_act["externalReferences"] = {
        "connection": CONFIG["warehouse_connection_id"]
    }
    
    SUMMARY.record_mapping(act.get('name'), adf_type, fabric_type)
    return new_act

def convert_hdinsight_activity(act):
    adf_type = act.get('type')
    fabric_type = "AzureHDInsight"
    
    hdi_activity_type = adf_type.replace("HDInsight", "") if adf_type. startswith("HDInsight") else "Unknown"
    
    new_act = _base_props(act, fabric_type)
    tp_old = act.get("typeProperties", {}) or {}
    
    new_act["typeProperties"] = {
        "hdiActivityType": hdi_activity_type,
        **deepcopy(tp_old)
    }
    
    SUMMARY.record_mapping(act. get('name'), adf_type, fabric_type)
    LOGGER.write_mapping(
        act.get('name'),
        adf_type,
        fabric_type,
        {"consolidation": f"{adf_type} -> AzureHDInsight (type: {hdi_activity_type})"}
    )
    
    return new_act

# ==========================================
# 7. CORE LOGIC
# ==========================================
def _base_props(old_act, new_type):
    return {
        "name": old_act.get("name", f"Unnamed_{new_type}"),
        "type": new_type,
        "dependsOn": deepcopy(old_act.get("dependsOn", [])),
        "policy": deepcopy(old_act.get("policy", {})),
        "userProperties": deepcopy(old_act.get("userProperties", []))
    }

def convert_activity_list(activities, pipeline_props=None, parent_path="root"):
    if not isinstance(activities, list):
        return []
    
    converted = []
    for idx, act in enumerate(activities):
        atype = act.get("type")
        name = act.get("name", f"unnamed_{idx}")
        path = f"{parent_path}.{name}({atype})"

        LOGGER.write_pre(path, act)

        fabric_type = ConnectorMapping.map_activity_type(atype)

        if atype == "DatabricksNotebook":
            new_act = convert_notebook(act)
        elif atype == "SqlServerStoredProcedure": 
            new_act = convert_stored_proc(act)
        elif atype == "ExecutePipeline":
            new_act = convert_invoke_pipeline(act)
        elif atype == "Copy":
            new_act = convert_copy(act, pipeline_props)
        elif atype == "Lookup":
            new_act = convert_lookup(act)
        elif atype == "GetMetadata":
            new_act = convert_get_metadata(act, pipeline_props)
        elif atype == "Delete":
            new_act = convert_delete(act, pipeline_props)
        elif atype == "Script":
            new_act = convert_script(act)
        elif atype == "SetVariable":
            new_act = convert_set_variable(act)
        elif atype == "ForEach":
            new_act = convert_for_each(act, pipeline_props)
        elif atype in ["HDInsightHive", "HDInsightPig", "HDInsightMapReduce", "HDInsightSpark", "HDInsightStreaming"]:
            new_act = convert_hdinsight_activity(act)
        else:
            new_act = deepcopy(act)
            if "linkedServiceName" in new_act: 
                del new_act["linkedServiceName"]
            LOGGER.write_mapping(path, atype or "Unknown", fabric_type, {"note": "passthrough + LS removed"})
            SUMMARY.record_mapping(path, atype or "Unknown", fabric_type)

        # Recurse into nested containers
        tp = new_act.get("typeProperties")
        if isinstance(tp, dict):
            if isinstance(tp.get("ifTrueActivities"), list):
                tp["ifTrueActivities"] = convert_activity_list(
                    tp["ifTrueActivities"],
                    pipeline_props,
                    parent_path=f"{path}.ifTrueActivities"
                )
            if isinstance(tp. get("ifFalseActivities"), list):
                tp["ifFalseActivities"] = convert_activity_list(
                    tp["ifFalseActivities"],
                    pipeline_props,
                    parent_path=f"{path}.ifFalseActivities"
                )
            if isinstance(tp.get("activities"), list):
                tp["activities"] = convert_activity_list(
                    tp["activities"],
                    pipeline_props,
                    parent_path=f"{path}.activities"
                )
            if isinstance(tp.get("cases"), list):
                tp["cases"] = [
                    {
                        **case,
                        "activities": convert_activity_list(
                            case. get("activities", []),
                            pipeline_props,
                            parent_path=f"{path}.cases[{i}].activities"
                        )
                    }
                    for i, case in enumerate(tp["cases"])
                ]
            if isinstance(tp.get("defaultActivities"), list):
                tp["defaultActivities"] = convert_activity_list(
                    tp["defaultActivities"],
                    pipeline_props,
                    parent_path=f"{path}. defaultActivities"
                )

        LOGGER.write_post(path, new_act)
        converted.append(new_act)

    return converted

def process_pipeline(source_json):
    props = deepcopy(source_json. get("properties", {}))
    
    if "variables" in props:
        for var_name, var_data in props["variables"].items():
            default_val = var_data.get("defaultValue")
            if isinstance(default_val, str) and ": :" in default_val:
                print(f"DEBUG:  Fixing typo in variable '{var_name}'")
                var_data["defaultValue"] = default_val.replace("::", ":")
    
    target = {
        "name": source_json.get("name", "ConvertedPipeline"),
        "objectId": str(uuid.uuid4()),
        "properties": props
    }
    
    activities = props.get("activities")
    if isinstance(activities, list):
        target["properties"]["activities"] = convert_activity_list(activities, props)
    else:
        target["properties"]["activities"] = []

    LOGGER.write_summary(SUMMARY. summary())
    return target

# ==========================================
# 8. CLI
# ==========================================
if __name__ == "__main__": 
    parser = argparse.ArgumentParser(
        description="ADF to Fabric Data Pipeline Converter with Comprehensive Copy Activity Mapping"
    )
    parser.add_argument("-f", "--file", required=True, help="Input ADF JSON file")
    parser.add_argument("-o", "--output", help="Output Fabric JSON file")
    parser.add_argument("--debug", action="store_true", help="Enable pre/post/mapping prints to stdout")
    parser.add_argument("--log", help="Write detailed conversion logs to this file")
    args = parser.parse_args()
    
    try:
        CONFIG["debug"] = bool(args.debug)
        LOGGER = Logger(path=args.log, also_stdout=CONFIG["debug"])

        with open(args.file, 'r', encoding='utf-8') as f:
            source_data = json.load(f)
        
        LOGGER._write_raw(f"Converting pipeline:  {source_data. get('name')}\n")
        LOGGER._write_raw(f"Using comprehensive mapping engine (50+ source/sink types)\n")

        result = process_pipeline(source_data)

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json. dump(result, f, indent=4)
            LOGGER._write_raw(f"\nSuccess! Output saved to {args.output}\n")
        else:
            LOGGER._write_raw("\n=== FINAL FABRIC JSON (stdout) ===\n")
            print(json.dumps(result, indent=4))

    except Exception as e:
        LOGGER._write_raw(f"Error: {str(e)}\n")
        LOGGER._write_raw(traceback.format_exc())
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        LOGGER.close()