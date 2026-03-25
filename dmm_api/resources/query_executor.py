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
    logger.info(f"Executing CSV query with software: {software}")
    try:
        # Query with DuckDB
        if software == "duckdb":
            s3_query = query
            # Match S3 paths with or without quotes
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", s3_query)
            logger.info(f"Found S3 paths in query: {s3_paths}")

            for s3_path in s3_paths:
                # Remove quotes if present
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                s3_query = s3_query.replace(s3_path, replacement)

            logger.info(f"Executing DuckDB query: {s3_query}")
            con = duckdb.connect(database=":memory:")
            result_df = con.execute(s3_query).fetchdf()
            logger.info(f"CSV query executed successfully, got {len(result_df)} rows")
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        logger.error(f"CSV query execution error: {str(e)}", exc_info=True)
        raise Exception(f"Query execution failed: {str(e)}")


def execute_query_postgres(query, duckdb_connection):
    """Execute query on PostgreSQL via DuckDB"""
    try:
        # Install and load postgres extension
        logger.info("Installing PostgreSQL extension...")
        duckdb_connection.sql("INSTALL postgres;")
        logger.info("PostgreSQL extension installed successfully")

        logger.info("Loading PostgreSQL extension...")
        duckdb_connection.sql("LOAD postgres;")
        logger.info("PostgreSQL extension loaded successfully")

        # Attach PostgreSQL database
        db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
        db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
        db_user = os.getenv("DS_READER_USER")
        db_password = os.getenv("DS_READER_PS")
        db_name = query["db_name"]

        # Log environment variables for debugging (with sensitive data masked)
        logger.info(
            f"PostgreSQL connection parameters: host={db_host}, port={db_port}, "
            f"db_name={db_name}, user={db_user}, password={'*' * len(db_password) if db_password else 'NOT SET'}"
        )

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

        logger.info(
            f"Attempting to attach PostgreSQL database: {db_name} at {db_host}:{db_port}"
        )
        duckdb_connection.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
        logger.info("PostgreSQL database attached successfully")

        logger.info(f"Executing query on PostgreSQL: {query.get("query")}")
        result_df = duckdb_connection.execute(query.get("query")).fetchdf()
        logger.info(f"Query executed successfully, got {len(result_df)} rows")

        return result_df

    except Exception as e:
        logger.error(f"PostgreSQL connection error: {str(e)}", exc_info=True)
        raise Exception(
            f"PostgreSQL connection failed: {str(e)}. "
            f"Check that: 1) Host is reachable (host={os.getenv('DATAGEMS_POSTGRES_HOST')}), "
            f"2) Port is correct (port={os.getenv('DATAGEMS_POSTGRES_PORT')}), "
            f"3) Credentials are valid (user={os.getenv('DS_READER_USER')}), "
            f"4) Database exists (db={os.getenv('DS_POSTGRES_DB', 'ds_era5_land')})"
        )


def execute_query_csv_postgres(query, software, args_sources=None):
    """Execute query with mixed CSV and PostgreSQL sources via DuckDB"""
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")

    if args_sources is None:
        args_sources = {}

    logger.info(f"Executing mixed query with software: {software}")
    logger.info(f"Args sources: {args_sources}")

    try:
        if software == "duckdb":
            # Create DuckDB connection
            con = duckdb.connect(database=":memory:")

            # If any args are from postgres, attach the database
            if any(source == "postgres" for source in args_sources.values()):
                logger.info("Attaching PostgreSQL database for mixed query...")
                con.sql("INSTALL postgres;")
                con.sql("LOAD postgres;")

                db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
                db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
                db_user = os.getenv("DS_READER_USER")
                db_password = os.getenv("DS_READER_PS")
                db_name = os.getenv("DS_POSTGRES_DB", "ds_era5_land")

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
                logger.info(
                    f"Attaching to PostgreSQL: {db_name} at {db_host}:{db_port}"
                )
                con.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
                logger.info("PostgreSQL attached successfully")

            # Handle CSV paths in query (convert S3 paths to local paths)
            processed_query = query
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", processed_query)
            logger.info(f"Found S3 paths in query: {s3_paths}")

            for s3_path in s3_paths:
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                processed_query = processed_query.replace(s3_path, replacement)

            # Execute the mixed query
            logger.info(f"Executing mixed query: {processed_query}")
            result_df = con.execute(processed_query).fetchdf()
            logger.info(f"Mixed query executed successfully, got {len(result_df)} rows")
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        logger.error(f"Mixed query execution error: {str(e)}", exc_info=True)
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
    logger.debug(
        f"Starting query rewriting with args_map: {args_map}, args_sources: {args_sources}"
    )

    for arg_name, arg_value in args_map.items():
        if isinstance(arg_value, str) and arg_value:
            source_type = args_sources.get(arg_name, "")
            logger.debug(
                f"Rewriting {arg_name}: value={arg_value}, source_type={source_type}"
            )

            # Replace both {{arg_name}} and {arg_name} patterns
            if source_type == "text/sql":  # PostgreSQL table reference
                replacement = arg_value  # No quotes, no s3://dataset/ prefix
                logger.debug(f"PostgreSQL table: {arg_name} -> {replacement}")
            elif source_type == "text/csv":  # CSV file path
                replacement = f"'{arg_value}'"  # With quotes for CSV paths
                logger.debug(f"CSV file: {arg_name} -> {replacement}")
            else:
                replacement = f"'{arg_value}'"  # Default with quotes
                logger.debug(f"Default (unknown source): {arg_name} -> {replacement}")

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

    logger.debug(f"Query after rewriting: {rewritten_query}")
    return rewritten_query


async def extract_query_from_AP(
    ap_payload: APRequest,
    token: str,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Dict[str, Any]:
    logger.info("Starting Analytical Pattern extraction...")
    try:
        logger.info("Parsing Analytical Pattern to graph...")
        G = json_to_graph(ap_payload)
        logger.info(
            f"Successfully parsed AP to graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges"
        )
    except Exception as e:
        logger.error(f"Failed to parse Analytical Pattern: {str(e)}", exc_info=True)
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

    logger.info("Classifying graph nodes by labels...")
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

    logger.info(
        f"Node classification: AP={len(AP_nodes)}, Operators={len(operator_nodes)}, Datasets={len(dataset_nodes)}, Users={len(user_nodes)}, FileObjects={len(file_object_nodes)}, DBConnections={len(db_connection_nodes)}"
    )

    if len(AP_nodes) != 1:
        logger.error(f"Expected 1 Analytical_Pattern node, found {len(AP_nodes)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Analytical_Pattern' node.",
        )

    if len(operator_nodes) < 1:
        logger.error("No SQL_Operator nodes found in Analytical Pattern")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'SQL_Operator' node.",
        )

    if len(dataset_nodes) < 1:
        logger.error("No Dataset nodes found in Analytical Pattern")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'Dataset' node.",
        )

    if len(user_nodes) != 1:
        logger.error(f"Expected 1 User node, found {len(user_nodes)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )

    # Get properties of datasets and database connections from MoMa and add them to the graph, as they are needed to execute the query
    logger.info(
        f"Fetching {len(file_object_nodes)} file object properties from MoMa2..."
    )
    for node_id in file_object_nodes:
        logger.info(f"  Fetching file object node: {node_id}")
        file_object_properties = await get_node_properties(node_id, token=token)
        G.nodes[node_id].update({"properties": file_object_properties})

    logger.info(
        f"Fetching {len(db_connection_nodes)} database connection properties from MoMa2..."
    )
    for node_id in db_connection_nodes:
        logger.info(f"  Fetching database connection node: {node_id}")
        dbc_properties = await get_node_properties(node_id, token=token)
        G.nodes[node_id].update({"properties": dbc_properties})

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    logger.info(f"AP properties retrieved - Process: {AP_process}")

    if expected_ap_process and AP_process != expected_ap_process:
        logger.error(
            f"AP Process mismatch: expected '{expected_ap_process}', got '{AP_process}'"
        )
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
        logger.error(f"Unsupported software: {query_info['software']}")
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
    logger.info(f"Extracted {len(args_map)} query arguments")

    for argname, node_id in args_map.items():
        args_sources[argname] = (
            G.nodes[node_id].get("properties", {}).get("encodingFormat", "")
        )

    logger.info(f"Argument sources: {args_sources}")

    # Store args_sources in query_info for later use
    query_info["args_sources"] = args_sources

    db_types = {s for s in args_sources.values() if s}  # Remove empty strings
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
        logger.info(f"Extracted database name: {db_name}")
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
            ## If the contentUrl as been generated locally, we miss the dataset_id, so we need to get it from the distribution edge
            if re.match(r"^s3/[^/]+\.(csv|json)$", args_map[argname]):
                dataset_id = next(
                    (
                        from_node
                        for from_node, to_node, edge_data in G.in_edges(
                            node_id, data=True
                        )
                        if "distribution" in edge_data.get("labels", [])
                    ),
                    None,
                )
                args_map[argname] = f"s3://dataset/{dataset_id}/{args_map[argname]}"

    logger.info("Rewriting query with extracted argument mappings...")
    query_info["query"] = query_rewriting(query_info["query"], args_map, args_sources)

    logger.info(f"Final rewritten query: {query_info['query']}")
    logger.info("Analytical Pattern extraction completed successfully")

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
        logger.info(f"Fetching node properties from MoMa2 - Node ID: {node_id}")
        logger.info(f"MoMa2 endpoint: {endpoint}")

        async with httpx.AsyncClient(
            timeout=MOMA_REQUEST_TIMEOUT_SECONDS, follow_redirects=True
        ) as client:
            logger.info(
                f"Sending GET request to MoMa2 with timeout: {MOMA_REQUEST_TIMEOUT_SECONDS}s"
            )
            response = await client.get(
                endpoint,
                headers={"Authorization": f"Bearer {token if token else 'NO_TOKEN'}"},
            )

        logger.info(f"MoMa2 response status: {response.status_code}")

        if response.status_code != 200:
            logger.warning(
                f"MoMa2 returned status {response.status_code} for node {node_id}"
            )
            logger.warning(
                f"Response body: {response.text[:500]}"
            )  # Log first 500 chars

        try:
            response_payload = response.json()
            logger.info(f"Successfully parsed JSON response for node {node_id}")
        except ValueError as e:
            logger.error(
                f"Failed to parse JSON response from MoMa2 for node {node_id}: {str(e)}"
            )
            logger.error(f"Response text: {response.text[:1000]}")
            response_payload = {
                "status_code": response.status_code,
                "content": response.text,
            }

        properties = response_payload.get("properties", {})
        logger.info(
            f"Extracted properties for node {node_id}: {list(properties.keys())}"
        )
        return properties

    except httpx.ConnectError as e:
        logger.error(f"Failed to connect to MoMa2 at {moma_api_url}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Cannot reach MoMa2 API at {moma_api_url}. Check MOMA_API_URL environment variable.",
        )
    except httpx.TimeoutException as e:
        logger.error(
            f"MoMa2 request timed out after {MOMA_REQUEST_TIMEOUT_SECONDS}s for node {node_id}: {str(e)}"
        )
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"MoMa2 API request timed out. Node: {node_id}",
        )
    except Exception as e:
        logger.error(
            f"Unexpected error fetching node properties from MoMa2 for node {node_id}: {str(e)}",
            exc_info=True,
        )
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
        logger.info("Starting query execution...")
        ap_payload = wrapped.ap
        query_info = await extract_query_from_AP(ap_payload, token=token)
        software = query_info.get("software")
        query_filled = query_info.get("query")
        db_connection = query_info.get("db_connection", "Unknown DB Connection")

        logger.info(
            f"Query info - Software: {software}, DB Connection: {db_connection}"
        )
        logger.info(f"Query to execute: {query_filled}")

        if db_connection == "postgres":
            logger.info("Attempting to execute PostgreSQL query...")
            result = execute_query_postgres(
                query_info, duckdb.connect(database=":memory:")
            )
        elif db_connection == "csv":
            logger.info(f"Attempting to execute CSV query with software: {software}...")
            result = execute_query_csv(query_filled, software)
        elif db_connection == "mixed":
            logger.info(
                f"Attempting to execute mixed query with software: {software}..."
            )
            result = execute_query_csv_postgres(
                query_filled, software, args_sources=query_info.get("args_sources", {})
            )
        else:
            logger.warning(
                f"Unexpected db_connection type: {db_connection}, defaulting to PostgreSQL"
            )
            result = execute_query_postgres(
                query_info, duckdb.connect(database=":memory:")
            )

        logger.info(f"Query execution successful, result has {len(result)} rows")
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
            logger.info(f"Dataset {dataset_id} sent to the AP Storage API.")
        except Exception as e:
            logger.warning(f"AP Storage API not working: {e}")

        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"Query executed successfully, results stored at {upload_path}",
            ap=AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query execution failed with error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )
