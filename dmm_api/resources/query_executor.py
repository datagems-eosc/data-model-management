# from datetime import datetime
import os
import re
from dmm_api.tools.AP.parse_AP import json_to_graph
from dmm_api.tools.AP.parse_AP import APRequest
import duckdb
import structlog
import json
from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, Optional

from ..tools.AP.generate_AP import generate_register_AP_after_query
from ..tools.AP.update_AP import update_AP_after_query, update_output_dataset_id

from ..tools.S3.results import upload_csv_to_results, upload_ap_to_results
from ..resources.dataset import APSuccessEnvelope, WrappedAPRequest, register_dataset


router = APIRouter()
logger = structlog.get_logger(__name__)


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


def query_rewriting(query: str, args_map: Dict[str, Any]) -> str:
    rewritten_query = query
    for arg_name, arg_value in args_map.items():
        if isinstance(arg_value, str):
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


def extract_query_from_AP(
    ap_payload: APRequest,
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

    AP_nodes, operator_nodes, dataset_nodes, file_object_nodes, user_nodes = (
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

    # operator_id = operator_nodes[0]
    # operator_properties = G.nodes[operator_id].get("properties", {})
    # operator_process = operator_properties.get("command")

    # if expected_operator_command and operator_process != expected_operator_command:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'. Operator node ID: '{operator_id}', properties: {operator_properties}",
    #     )

    if query_info["software"] not in ["DuckDB", "Ontop"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported software: {query_info['software']}. Supported software are 'DuckDB' and 'Ontop'.",
        )

    args_map = {
        data.get("properties", {}).get("argname"): u
        for u, v, data in G.edges(data=True)
        if "argname" in data.get("properties", {})
    }

    for argname, node_id in args_map.items():
        if G.in_edges(node_id, data=True):
            for u, _, data in G.in_edges(node_id, data=True):
                if "distribution" in data.get("labels", {}):
                    args_map[argname] = (
                        u
                        + "/"
                        + args_map[argname]
                        + "/"
                        + G.nodes[node_id].get("properties", {}).get("name", "")
                    )
                    break

    print(f"Args map: {args_map}")

    # Extract S3 paths BEFORE query rewriting
    s3_paths = re.findall(r"s3://dataset/[^\s,;'\"]+", query_info["query"])
    print(f"Found S3 paths in original query: {s3_paths}")

    query_info["query"] = query_rewriting(query_info["query"], args_map)
    print(f"Rewritten query: {query_info['query']}")

    # TODO: change code to work with new AP structure
    # parameters = operator_properties.get("Parameters", {}) or {}
    # args_map: Dict[str, Any] = {}
    # for k, v in parameters.items():
    #     if k.startswith("arg"):
    #         args_map[k] = v

    # resolved_args: Dict[str, Any] = {}
    # for name, value in args_map.items():
    #     resolved = value
    #     try:
    #         if isinstance(value, str) and value in G.nodes:
    #             props = G.nodes[value].get("properties", {}) or {}
    #             resolved = props.get("contentUrl") or resolved
    #     except Exception:
    #         resolved = value
    #     resolved_args[name] = resolved

    # filled_query = raw_query or ""
    # for name, value in resolved_args.items():
    #     filled_query = re.sub(
    #         r"\{\{\s*" + re.escape(name) + r"\s*\}\}", str(value), filled_query
    #     )
    #     filled_query = re.sub(
    #         r"\{\s*" + re.escape(name) + r"\s*\}", str(value), filled_query
    #     )

    # query_info["args"] = args_map
    # query_info["query_filled"] = filled_query

    return query_info


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501


@router.post(
    "/polyglot/query",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True,
)
async def execute_query(wrapped: WrappedAPRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        ap_payload = wrapped.ap
        query_info = extract_query_from_AP(ap_payload)
        software = query_info.get("software")
        query_filled = query_info.get("query")

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
        await register_dataset(register_AP)

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
