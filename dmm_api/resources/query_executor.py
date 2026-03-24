# from datetime import datetime
import os
import re
from dmm_api.resources import security
from dmm_api.tools.AP.parse_AP import json_to_graph
from dmm_api.tools.AP.parse_AP import APRequest
import duckdb
import structlog
import json
import httpx
from fastapi import APIRouter, HTTPException, status, Depends
from typing import Dict, Any, Optional

from ..tools.AP.generate_AP import generate_register_AP_after_query
from ..tools.AP.update_AP import update_AP_after_query, update_output_dataset_id

from ..tools.S3.results import upload_csv_to_results, upload_ap_to_results
from ..resources.dataset import APSuccessEnvelope, WrappedAPRequest, register_dataset


router = APIRouter()
logger = structlog.get_logger(__name__)

MOMA_REQUEST_TIMEOUT_SECONDS = 30.0
MOMA_URL = os.getenv("MOMA_URL", "https://datagems-dev.scayle.es/moma2/api/v1")


# We will implement different query execution methods based on the data type
def execute_query_csv(query, software):
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    print(f"Executing query with software: {software}")
    try:
        # Query with DuckDB
        if software == "duckdb":
            s3_query = query
            # Match S3 paths with or without quotes
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", s3_query)
            print(f"Found S3 paths in query: {s3_paths}")

            for s3_path in s3_paths:
                # Remove quotes if present
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                s3_query = s3_query.replace(s3_path, replacement)

            con = duckdb.connect(database=":memory:")
            result_df = con.execute(s3_query).fetchdf()
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")


def execute_query_postgres(query, duckdb_connection):
    """Execute query on PostgreSQL via DuckDB"""
    # Install and load postgres extension
    duckdb_connection.sql("INSTALL postgres;")
    duckdb_connection.sql("LOAD postgres;")

    # Attach PostgreSQL database
    db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
    db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
    db_user = os.getenv("DS_READER_USER")
    db_password = os.getenv("DS_READER_PS")
    db_name = os.getenv("DS_POSTGRES_DB", "ds_era5_land")

    connection_string = (
        f"dbname={db_name} user={db_user} password={db_password} "
        f"host={db_host} port={db_port}"
    )

    duckdb_connection.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
    result_df = duckdb_connection.execute(query).fetchdf()

    return result_df


def execute_query_csv_postgres(query, software, args_sources=None):
    """Execute query with mixed CSV and PostgreSQL sources via DuckDB"""
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")

    if args_sources is None:
        args_sources = {}

    print(f"Executing mixed query with software: {software}")

    try:
        if software == "duckdb":
            # Create DuckDB connection
            con = duckdb.connect(database=":memory:")

            # If any args are from postgres, attach the database
            if any(source == "postgres" for source in args_sources.values()):
                print("Attaching PostgreSQL database...")
                con.sql("INSTALL postgres;")
                con.sql("LOAD postgres;")

                db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
                db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
                db_user = os.getenv("DS_READER_USER")
                db_password = os.getenv("DS_READER_PS")
                db_name = os.getenv("DS_POSTGRES_DB", "ds_era5_land")

                connection_string = (
                    f"dbname={db_name} user={db_user} password={db_password} "
                    f"host={db_host} port={db_port}"
                )
                con.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")

            # Handle CSV paths in query (convert S3 paths to local paths)
            processed_query = query
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", processed_query)
            print(f"Found S3 paths in query: {s3_paths}")

            for s3_path in s3_paths:
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                processed_query = processed_query.replace(s3_path, replacement)

            # Execute the mixed query
            result_df = con.execute(processed_query).fetchdf()
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        raise Exception(f"Mixed query execution failed: {str(e)}")


def query_rewriting(
    query: str, args_map: Dict[str, Any], db_connection: str = "csv"
) -> str:
    rewritten_query = query
    for arg_name, arg_value in args_map.items():
        if isinstance(arg_value, str):
            # For PostgreSQL, format as table reference
            rewritten_query = re.sub(
                r"\{\{\s*" + re.escape(arg_name) + r"\s*\}\}",
                f"'s3://dataset/{arg_value}'",
                rewritten_query,
            )
            rewritten_query = re.sub(
                r"\{\s*" + re.escape(arg_name) + r"\s*\}",
                f"'s3://dataset/{arg_value}'",
                rewritten_query,
            )
    return rewritten_query


async def extract_query_from_AP(
    ap_payload: APRequest,
    token: str,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        G = json_to_graph(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )

    (
        AP_nodes,
        operator_nodes,
        dataset_nodes,
        file_object_nodes,
        user_nodes,
        db_connection_nodes,
    ) = (
        [],
        [],
        [],
        [],
        [],
        [],
    )
    for node_id, attributes in G.nodes(data=True):
        labels = attributes.get("labels", [])
        if "Analytical_Pattern" in labels:
            AP_nodes.append(node_id)
        elif "SQL_Operator" in labels:
            operator_nodes.append(node_id)
        elif "sc:Dataset" in labels:
            dataset_nodes.append(node_id)
        elif "User" in labels:
            user_nodes.append(node_id)
        elif "cr:FileObject" in labels:
            file_object_nodes.append(node_id)
        elif "dg:DatabaseConnection" in labels:
            db_connection_nodes.append(node_id)

    if len(AP_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Analytical_Pattern' node.",
        )

    if len(operator_nodes) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'SQL_Operator' node.",
        )

    if len(dataset_nodes) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'Dataset' node.",
        )

    if len(user_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )

    # Get properties of datasets and database connections from MoMa and add them to the graph, as they are needed to execute the query
    for node_id in dataset_nodes:
        dataset_properties = await get_node_properties(node_id, token=token)
        G.nodes[node_id].update({"properties": dataset_properties})

    for node_id in db_connection_nodes:
        dbc_properties = await get_node_properties(node_id, token=token)
        G.nodes[node_id].update({"properties": dbc_properties})

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'. AP node ID: '{AP_id}', properties: {AP_properties}",
        )

    query_info: Dict[str, Any] = {}
    query_info["software"] = (
        G.nodes[operator_nodes[0]]
        .get("properties", {})
        .get("name", "Unknown Software")
        .split(" ")[0]
    )
    query_info["query"] = (
        G.nodes[operator_nodes[0]].get("properties", {}).get("query", "No query found")
    )
    logger.info(
        f"Extracted software: {query_info['software']}, query: {query_info['query']}"
    )

    if query_info["software"] not in ["DuckDB", "Ontop"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported software: {query_info['software']}. Supported software are 'DuckDB' and 'Ontop'.",
        )

    args_sources = {}
    args_map = {
        data.get("properties", {}).get("argname"): u
        for u, v, data in G.edges(data=True)
        if "argname" in data.get("properties", {})
    }
    for argname, node_id in args_map.items():
        args_sources[argname] = (
            G.nodes[node_id].get("properties", {}).get("encodingFormat", "")
        )

    # Extract database name from DatabaseConnection node
    db_name = None
    if db_connection_nodes:
        db_properties = G.nodes[db_connection_nodes[0]].get("properties", {})
        db_name = db_properties.get("name", "ds_era5_land")
        logger.info(f"Extracted database name: {db_name}")
        query_info["db_name"] = db_name
        query_info["db_connection"] = "postgres"

    # Apply original CSV logic with distribution label handling
    if query_info["db_connection"] == "csv":
        for argname, node_id in args_map.items():
            args_map[argname] = (
                f"{G.nodes[node_id].get('properties', {}).get('contentUrl', '')}"
            )

    # For PostgreSQL, extract table names from FileObject properties
    if query_info["db_connection"] == "postgres":
        for argname, node_id in args_map.items():
            if node_id in file_object_nodes:
                # Get the FileObject name (table name)
                file_object_name = (
                    G.nodes[node_id].get("properties", {}).get("name", "")
                )
                args_map[argname] = file_object_name
                logger.info(f"Mapped PostgreSQL table {argname} -> {file_object_name}")

    ##TODO: Get the file name from MoMa instead of having it in the AP

    query_info["query"] = query_rewriting(
        query_info["query"], args_map, query_info["db_connection"]
    )

    return query_info


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501


# Get the properties of a node from MoMa given its ID. This is needed to get the contentUrl of the datasets, which is required to execute the query
async def get_node_properties(node_id, token: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    ) as client:
        response = await client.post(
            f"{os.getenv('MOMA_API_URL', 'http://localhost:8000')}/nodes/{node_id}",
            headers={"Authorization": f"Bearer {token}"},
        )

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {
            "status_code": response.status_code,
            "content": response.text,
        }
    return response_payload.get("properties", {})


@router.post(
    "/polyglot/query",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True,
)
async def execute_query(
    wrapped: WrappedAPRequest,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        ap_payload = wrapped.ap
        query_info = await extract_query_from_AP(ap_payload, token=token)
        software = query_info.get("software")
        query_filled = query_info.get("query")

        if query_info["db_connection"] == "postgres":
            result = execute_query_postgres(
                query_filled, duckdb.connect(database=":memory:")
            )
        else:
            result = execute_query_csv(query_filled, software)

        ap_payload, dataset_id = update_output_dataset_id(ap_payload)
        csv_bytes = result.to_csv(index=False).encode("utf-8")
        upload_path, dataset_id = upload_csv_to_results(csv_bytes, dataset_id)

        AP_query_after = update_AP_after_query(ap_payload, dataset_id, upload_path)
        upload_ap_to_results(
            json.dumps(
                AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
                ensure_ascii=False,
                indent=2,
            ),
            dataset_id,
        )

        register_AP = generate_register_AP_after_query(AP_query_after)
        await register_dataset(WrappedAPRequest(ap=register_AP))

        # Fake forward to AP Storage API
        try:
            print(f"Dataset {dataset_id} sent to the AP Storage API.")
        except Exception as e:
            print(f"AP Storage API not working: {e}")

        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"Query executed successfully, results stored at {upload_path}",
            ap=AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )
