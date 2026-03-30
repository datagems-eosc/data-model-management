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
MOMA_API_URL = os.getenv("MOMA_API_URL", "https://datagems-dev.scayle.es/moma2/api/v1")


# We will implement different query execution methods based on the data type
def execute_query_csv(query, software):
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    try:
        # Query with DuckDB
        if software == "duckdb":
            s3_query = query
            # Match S3 paths with or without quotes
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", s3_query)
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
    try:
        # Install and load postgres extension
        duckdb_connection.sql("INSTALL postgres;")
        duckdb_connection.sql("LOAD postgres;")

        # Attach PostgreSQL database
        db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
        db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
        db_user = os.getenv("DS_READER_USER")
        db_password = os.getenv("DS_READER_PS")
        db_name = query["db_name"]

        # Check if required environment variables are set
        missing_vars = []
        if not db_host:
            missing_vars.append("DATAGEMS_POSTGRES_HOST")
        if not db_port:
            missing_vars.append("DATAGEMS_POSTGRES_PORT")
        if not db_user:
            missing_vars.append("DS_READER_USER")
        if not db_password:
            missing_vars.append("DS_READER_PS")

        if missing_vars:
            raise ValueError(
                f"Missing PostgreSQL environment variables: {', '.join(missing_vars)}. "
                f"Please ensure these are set in your docker run command or environment."
            )

        connection_string = (
            f"dbname={db_name} user={db_user} password={db_password} "
            f"host={db_host} port={db_port}"
        )

        duckdb_connection.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
        result_df = duckdb_connection.execute(query.get("query")).fetchdf()

        return result_df

    except Exception as e:
        raise Exception(
            f"PostgreSQL connection failed: {str(e)}. "
            f"Check that: 1) Host is reachable (host={os.getenv('DATAGEMS_POSTGRES_HOST')}), "
            f"2) Port is correct (port={os.getenv('DATAGEMS_POSTGRES_PORT')}), "
            f"3) Credentials are valid (user={os.getenv('DS_READER_USER')}), "
            f"4) Database exists (db={query.get('db_name')})"
        )


def execute_query_csv_postgres(query, software, args_sources=None, db_name=None):
    """Execute query with mixed CSV and PostgreSQL sources via DuckDB"""
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    if args_sources is None:
        args_sources = {}
    try:
        if software == "duckdb":
            # Create DuckDB connection
            con = duckdb.connect(database=":memory:")

            # If any args are from postgres, attach the database
            if any(source == "text/sql" for source in args_sources.values()):
                con.sql("INSTALL postgres;")
                con.sql("LOAD postgres;")

                db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
                db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
                db_user = os.getenv("DS_READER_USER")
                db_password = os.getenv("DS_READER_PS")
                db_name = query["db_name"]

                # Check if required environment variables are set
                missing_vars = []
                if not db_host:
                    missing_vars.append("DATAGEMS_POSTGRES_HOST")
                if not db_port:
                    missing_vars.append("DATAGEMS_POSTGRES_PORT")
                if not db_user:
                    missing_vars.append("DS_READER_USER")
                if not db_password:
                    missing_vars.append("DS_READER_PS")

                if missing_vars:
                    raise ValueError(
                        f"Missing PostgreSQL environment variables: {', '.join(missing_vars)}"
                    )

                connection_string = (
                    f"dbname={db_name} user={db_user} password={db_password} "
                    f"host={db_host} port={db_port}"
                )
                con.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")

            # Handle CSV paths in query (convert S3 paths to local paths)
            processed_query = query.get("query", "")
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", processed_query)

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
    query: str, args_map: Dict[str, Any], args_sources: Dict[str, str] = None
) -> str:
    """
    Rewrite query placeholders with actual values.

    For PostgreSQL tables: {{arg1}} -> table_name (without quotes)
    For CSV files: {{arg1}} -> 's3://dataset/path/file.csv' (with quotes)
    """
    if args_sources is None:
        args_sources = {}

    rewritten_query = query

    for arg_name, arg_value in args_map.items():
        if isinstance(arg_value, str) and arg_value:
            source_type = args_sources.get(arg_name, "")
            # Replace both {{arg_name}} and {arg_name} patterns
            if source_type == "text/sql":  # PostgreSQL table reference
                replacement = arg_value  # No quotes, no s3://dataset/ prefix
            elif source_type == "text/csv":  # CSV file path
                replacement = f"'{arg_value}'"  # With quotes for CSV paths
            else:
                replacement = f"'{arg_value}'"  # Default with quotes

            # Replace {{arg_name}} pattern
            rewritten_query = re.sub(
                r"\{\{\s*" + re.escape(arg_name) + r"\s*\}\}",
                replacement,
                rewritten_query,
            )
            # Replace {arg_name} pattern (alternative syntax)
            rewritten_query = re.sub(
                r"\{\s*" + re.escape(arg_name) + r"\s*\}",
                replacement,
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

    for node_id in db_connection_nodes:
        dbc_properties = await get_node_properties(node_id, token=token)
        current_properties = G.nodes[node_id].get("properties", {})
        current_properties.update(dbc_properties)
        G.nodes[node_id].update({"properties": current_properties})

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

    for argname in args_map.keys():
        node_id = args_map[argname]
        file_object_properties = await get_node_properties(node_id, token=token)

        G.nodes[node_id].update({"properties": file_object_properties})
        logger.info(f"Arg '{argname}' is updated)")

    for argname, node_id in args_map.items():
        args_sources[argname] = (
            G.nodes[node_id].get("properties", {}).get("encodingFormat", "")
        )

    # Store args_sources in query_info for later use
    query_info["args_sources"] = args_sources

    # Map source types to connection types
    db_types = set()
    for source in args_sources.values():
        if source == "text/sql":
            db_types.add("postgres")
        elif source == "text/csv":
            db_types.add("csv")

    query_info["db_connection"] = (
        "mixed"
        if len(db_types) > 1
        else next(iter(db_types))
        if db_types
        else "unknown"
    )
    # Extract database name from DatabaseConnection node
    db_name = None
    if db_connection_nodes:
        db_properties = G.nodes[db_connection_nodes[0]].get("properties", {})
        db_name = db_properties.get("name", "ds_era5_land")
        query_info["db_name"] = db_name

    for argname, source in args_sources.items():
        if source == "text/sql":
            args_map[argname] = "pg_db.public." + G.nodes[args_map[argname]].get(
                "properties", {}
            ).get("name", "")
        elif source == "text/csv":
            node_id = args_map[argname]
            args_map[argname] = (
                G.nodes[args_map[argname]].get("properties", {}).get("contentUrl", "")
            )
            # ## If the contentUrl as been generated locally, we miss the dataset_id, so we need to get it from the distribution edge
            # if re.match(r"^s3:/?[^/]+\.(csv|json)$", args_map[argname]):
            #     dataset_id = next(
            #         (
            #             from_node
            #             for from_node, to_node, edge_data in G.in_edges(
            #                 node_id, data=True
            #             )
            #             if "distribution" in edge_data.get("labels", [])
            #         ),
            #         None,
            #     )
            #     # Strip the s3:/ or s3:// prefix from the filename, then rebuild with dataset_id
            #     filename = re.sub(r"^s3:/?/?", "", args_map[argname])
            #     args_map[argname] = f"s3://dataset/{dataset_id}/{filename}"

    query_info["query"] = query_rewriting(query_info["query"], args_map, args_sources)
    return query_info


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501


# Get the properties of a node from MoMa given its ID. This is needed to get the contentUrl of the datasets, which is required to execute the query
async def get_node_properties(node_id, token: Optional[str] = None) -> Dict[str, Any]:
    """Fetch node properties from MoMa2 API"""
    moma_api_url = os.getenv(
        "MOMA_API_URL", "https://datagems-dev.scayle.es/moma2/v1/api"
    )
    endpoint = f"{moma_api_url}/nodes/{node_id}"
    try:
        async with httpx.AsyncClient(
            timeout=MOMA_REQUEST_TIMEOUT_SECONDS, follow_redirects=True
        ) as client:
            response = await client.get(
                endpoint,
                headers={"Authorization": f"Bearer {token if token else 'NO_TOKEN'}"},
            )

        # Check if node was not found
        if response.status_code == 404:
            logger.error(
                f"Node with ID '{node_id}' not found in MoMa2. Response: {response.text}"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The fileObject/databaseConnection have not been found in MoMa (node ID: '{node_id}'). Please make sure that the dataset is well onboarded.",
            )

        # Check for other HTTP errors
        if not response.is_success:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"MoMa2 API error {response.status_code} for node '{node_id}': {response.text}",
            )

        try:
            response_payload = response.json()
        except ValueError:
            response_payload = {
                "status_code": response.status_code,
                "content": response.text,
            }

        properties = response_payload.get("properties", {})
        return properties

    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Cannot reach MoMa2 API at {moma_api_url}. Check MOMA_API_URL environment variable.",
        )
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"MoMa2 API request timed out. Node: {node_id}",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching node properties from MoMa2: {str(e)}",
        )


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
        db_connection = query_info.get("db_connection", "Unknown DB Connection")

        if db_connection == "postgres":
            result = execute_query_postgres(
                query_info, duckdb.connect(database=":memory:")
            )
        elif db_connection == "csv":
            result = execute_query_csv(query_filled, software)
        elif db_connection == "mixed":
            result = execute_query_csv_postgres(
                query_info,
                software,
                args_sources=query_info.get("args_sources", {}),
                db_name=query_info.get("db_name"),
            )
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

        # TODO: AP Storage

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
