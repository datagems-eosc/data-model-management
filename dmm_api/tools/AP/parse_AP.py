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
    # Make properties optional with default empty dict
    properties: Dict[str, Any] = Field(default_factory=dict)


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
    operator_process = operator_properties.get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'. Operator node ID: '{operator_id}', properties: {operator_properties}",
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
    raw_query = operator_properties.get("Query")
    query_info["query"] = raw_query
    software_prop = operator_properties.get("Software", {})
    query_info["software"] = software_prop.get("name")

    if query_info["software"] not in ["DuckDB", "Ontop"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported software: {query_info['software']}. Supported software are 'DuckDB' and 'Ontop'.",
        )

    # TODO: change code to work with new AP structure
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


# Add recordSet too
def extract_datasets_from_AP(
    ap_payload: APRequest,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
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

    original_dataset_ids = list(dataset_nodes)
    relabel_map: Dict[str, str] = {}
    for did in dataset_nodes:
        if not is_valid_uuid(did):
            relabel_map[did] = str(uuid.uuid4())

    if relabel_map:
        G = nx.relabel_nodes(G, relabel_map)
    final_dataset_ids = [relabel_map.get(did, did) for did in dataset_nodes]
    # old_to_new = {old: relabel_map.get(old, old) for old in dataset_nodes}

    operator_id = operator_nodes[0]
    operator_properties = G.nodes[operator_id].get("properties", {})
    operator_process = operator_properties.get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'. Operator node ID: '{operator_id}', properties: {operator_properties}",
        )

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'. AP node ID: '{AP_id}', properties: {AP_properties}",
        )

    datasets: List[Dict[str, Any]] = []
    missing_archived = []
    for dataset_id in final_dataset_ids:
        dataset_properties = G.nodes[dataset_id].get("properties", {}).copy()
        if "archivedAt" not in dataset_properties:
            missing_archived.append(dataset_id)
            continue

        dataset = {
            "@context": {**CONTEXT_TEMPLATE, **REFERENCES_TEMPLATE},
            "@id": dataset_id,
        }
        dataset.update(dataset_properties)

        distribution = []
        try:
            for _, to_id, edata in G.out_edges(dataset_id, data=True):
                edge_labels = edata.get("labels", []) or []
                if "distribution" not in edge_labels:
                    continue
                node_attrs = G.nodes.get(to_id, {}) or {}
                node_props = node_attrs.get("properties", {}) or {}
                node_labels = node_attrs.get("labels", []) or []

                file_obj: Dict[str, Any] = {"@id": to_id}
                file_obj["@type"] = node_props.get("@type") or next(
                    (
                        lbl
                        for lbl in node_labels
                        if isinstance(lbl, str) and lbl.lower().endswith("fileobject")
                    ),
                    "cr:FileObject",
                )
                file_obj.update(node_props)

                distribution.append(file_obj)
        except Exception:
            distribution = []

        if distribution:
            dataset["distribution"] = distribution

        datasets.append(dataset)

    if missing_archived:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The following Dataset nodes are missing 'archivedAt': {missing_archived}",
        )

    return datasets, original_dataset_ids


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
    dataset_label = "sc:Dataset"
    for node_id, attributes in G.nodes(data=True):
        if dataset_label in attributes.get("labels", []):
            dataset_nodes.append(node_id)

    if len(dataset_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The Analytical Pattern must contain exactly one '{dataset_label}' node.",
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
    dataset_label = "sc:Dataset"
    for node_id, attributes in G.nodes(data=True):
        labels = attributes.get("labels", [])
        if "Analytical_Pattern" in labels:
            AP_nodes.append(node_id)
        elif "DataModelManagement_Operator" in labels:
            operator_nodes.append(node_id)
        elif dataset_label in labels:
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
            detail=f"The Analytical Pattern must contain exactly one '{dataset_label}' node.",
        )

    if len(user_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )
    dataset_id = dataset_nodes[0]
    dataset_properties = G.nodes[dataset_id].get("properties", {}).copy()

    operator_id = operator_nodes[0]
    operator_properties = G.nodes[operator_id].get("properties", {})
    operator_process = operator_properties.get("command")

    if expected_operator_command and operator_process != expected_operator_command:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Operator 'command'='{expected_operator_command}', but found '{operator_process}'. Operator node ID: '{operator_id}', properties: {operator_properties}",
        )

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'. AP node ID: '{AP_id}', properties: {AP_properties}",
        )

    return dataset_properties.get("archivedAt")
    return dataset_properties.get("archivedAt")
