import re
import networkx as nx
from fastapi import HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Tuple
import uuid
from dmm_api.config.constants import (
    CONTEXT_TEMPLATE,
    REFERENCES_TEMPLATE,
)


class Node(BaseModel):
    id: str | int = Field(..., alias="@id")
    labels: List[str]
    properties: Dict[str, Any]


class Edge(BaseModel):
    source: str | int = Field(..., alias="from")
    target: str | int = Field(..., alias="to")
    labels: List[str] = []


class APRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]
    directed: bool = True
    multigraph: bool = True
    # graph: Dict[str, Any] = {}


# TODO: ErrorEnvelope


# check if a string is a valid UUID and version 4
def is_valid_uuid(value):
    try:
        uuid.UUID(value, version=4)
        return True
    except ValueError:
        return False


def json_to_graph(query_data: APRequest) -> nx.DiGraph | nx.MultiDiGraph:
    graph_data = query_data.model_dump(by_alias=True)

    G = nx.MultiDiGraph() if graph_data.get("multigraph", True) else nx.DiGraph()

    for node in graph_data["nodes"]:
        G.add_node(
            node["@id"],
            labels=node.get("labels", []),
            properties=node.get("properties", {}),
        )

    for edge in graph_data["edges"]:
        G.add_edge(edge["from"], edge["to"], labels=edge.get("labels", []))

    return G


# TODO: delete this function
# Simplified case with only one SQL_Operator, Dataset and FileObject in the AP
def extract_from_AP(query_data: APRequest):
    try:
        G = json_to_graph(query_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse Analytical Pattern into a graph: {str(e)}",
        )

    extracted_data = {}

    operator_id = None
    for node_id, attributes in G.nodes(data=True):
        if "SQL_Operator" in attributes.get("labels", []):
            operator_id = node_id
            break
    if operator_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No SQL_Operator node found in the Analytical Pattern.",
        )
    sql_op_properties = G.nodes[operator_id].get("properties", {})
    extracted_data["query"] = sql_op_properties.get("Query")
    software_prop = sql_op_properties.get("Software", {})
    extracted_data["software"] = software_prop.get("name")

    dataset_neighbors = []
    for successor_id in G.successors(operator_id):
        node_labels = G.nodes[successor_id].get("labels", [])
        if "sc:Dataset" in node_labels:
            dataset_neighbors.append(successor_id)
    if not dataset_neighbors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No Dataset node connected to the SQL_Operator.",
        )
    dataset_node_id = dataset_neighbors[0]
    dataset_properties = G.nodes[dataset_node_id].get("properties", {})
    extracted_data["dataset_name"] = dataset_properties.get("name")

    fileobject_neighbors = []
    for successor_id in G.successors(dataset_node_id):
        node_labels = G.nodes[successor_id].get("labels", [])
        if "FileObject" in node_labels:
            fileobject_neighbors.append(successor_id)
    if not fileobject_neighbors:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No FileObject node connected to the Dataset.",
        )
    fileobject_id = fileobject_neighbors[0]
    file_properties = G.nodes[fileobject_id].get("properties", {})
    extracted_data["dataset_id"] = fileobject_id
    extracted_data["csv_name"] = file_properties.get("name")
    extracted_data["contentUrl"] = file_properties.get("contentUrl")

    user_predecessors = []
    for predecessor_id in G.predecessors(operator_id):
        node_labels = G.nodes[predecessor_id].get("labels", [])
        if "User" in node_labels:
            user_predecessors.append(predecessor_id)
    if len(user_predecessors) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one User node connected to the SQL_Operator.",
        )
    extracted_data["user_id"] = user_predecessors[0]

    return extracted_data


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

    AP_nodes, operator_nodes, dataset_nodes, user_nodes = [], [], [], []
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

    operator_id = operator_nodes[0]
    operator_properties = G.nodes[operator_id].get("properties", {})
    operator_process = operator_properties.get("Parameters", {}).get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'.",
        )

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'.",
        )

    query_info: Dict[str, Any] = {}
    raw_query = operator_properties.get("Query")
    query_info["query"] = raw_query
    software_prop = operator_properties.get("Software", {})
    query_info["software"] = software_prop.get("name")

    if query_info["software"] not in ["DuckDB", "Ontop"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported software: {query_info['software']}. Supported software are 'DuckDB' and 'Ontop'.",
        )

    parameters = operator_properties.get("Parameters", {}) or {}
    args_map: Dict[str, Any] = {}
    for k, v in parameters.items():
        if k.startswith("arg"):
            args_map[k] = v

    resolved_args: Dict[str, Any] = {}
    for name, value in args_map.items():
        resolved = value
        try:
            if isinstance(value, str) and value in G.nodes:
                props = G.nodes[value].get("properties", {}) or {}
                resolved = props.get("contentUrl") or resolved
        except Exception:
            resolved = value
        resolved_args[name] = resolved

    filled_query = raw_query or ""
    for name, value in resolved_args.items():
        filled_query = re.sub(
            r"\{\{\s*" + re.escape(name) + r"\s*\}\}", str(value), filled_query
        )
        filled_query = re.sub(
            r"\{\s*" + re.escape(name) + r"\s*\}", str(value), filled_query
        )

    query_info["args"] = args_map
    query_info["query_filled"] = filled_query

    return query_info


# TODO: check the edges too
def extract_dataset_from_AP(
    ap_payload: APRequest,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:
    try:
        G = json_to_graph(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )

    AP_nodes, operator_nodes, dataset_nodes, user_nodes = [], [], [], []
    for node_id, attributes in G.nodes(data=True):
        labels = attributes.get("labels", [])
        if "Analytical_Pattern" in labels:
            AP_nodes.append(node_id)
        elif "DataModelManagement_Operator" in labels:
            operator_nodes.append(node_id)
        elif "sc:Dataset" in labels:
            dataset_nodes.append(node_id)
        elif "User" in labels:
            user_nodes.append(node_id)

    if len(AP_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Analytical_Pattern' node.",
        )

    if len(operator_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'DataModelManagement_Operator' node.",
        )

    if len(dataset_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Dataset' node.",
        )

    if len(user_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )

    # If the dataset_id is not a valid UUID, generate a new one
    dataset_id = dataset_nodes[0]
    old_dataset_id = dataset_id

    if not is_valid_uuid(dataset_id):
        new_dataset_id = str(uuid.uuid4())
        G = nx.relabel_nodes(G, {dataset_id: new_dataset_id})
        dataset_id = new_dataset_id

    dataset_properties = G.nodes[dataset_id].get("properties", {}).copy()
    # Check that the dataset has the field 'archivedAt'
    if "archivedAt" not in dataset_properties:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Dataset node must contain the 'archivedAt' property.",
        )

    operator_properties = G.nodes[operator_nodes[0]].get("properties", {})
    operator_process = operator_properties.get("Parameters", {}).get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'.",
        )

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'.",
        )

    dataset = {
        "@context": {**CONTEXT_TEMPLATE, **REFERENCES_TEMPLATE},
        "@id": dataset_id,
    }
    dataset.update(dataset_properties)

    return dataset, old_dataset_id


def extract_dataset_id_from_AP(
    ap_payload: APRequest,
) -> str:
    try:
        G = json_to_graph(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )
    dataset_nodes = []
    for node_id, attributes in G.nodes(data=True):
        if "sc:Dataset" in attributes.get("labels", []):
            dataset_nodes.append(node_id)

    if len(dataset_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Dataset' node.",
        )

    dataset_id = dataset_nodes[0]
    return dataset_id


def extract_dataset_path_from_AP(
    ap_payload: APRequest,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:
    try:
        G = json_to_graph(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )

    AP_nodes, operator_nodes, dataset_nodes, user_nodes = [], [], [], []
    for node_id, attributes in G.nodes(data=True):
        labels = attributes.get("labels", [])
        if "Analytical_Pattern" in labels:
            AP_nodes.append(node_id)
        elif "DataModelManagement_Operator" in labels:
            operator_nodes.append(node_id)
        elif "sc:Dataset" in labels:
            dataset_nodes.append(node_id)
        elif "User" in labels:
            user_nodes.append(node_id)

    if len(AP_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Analytical_Pattern' node.",
        )

    if len(operator_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'DataModelManagement_Operator' node.",
        )

    if len(dataset_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Dataset' node.",
        )

    if len(user_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )

    dataset_id = dataset_nodes[0]
    dataset_properties = G.nodes[dataset_id].get("properties", {}).copy()

    operator_properties = G.nodes[operator_nodes[0]].get("properties", {})
    operator_process = operator_properties.get("Parameters", {}).get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'.",
        )

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'.",
        )

    return dataset_properties.get("archivedAt")
