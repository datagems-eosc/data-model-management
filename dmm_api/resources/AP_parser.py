import networkx as nx
from fastapi import HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Dict, Any
import json
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
    graph: Dict[str, Any] = {}


def is_valid_uuid_strict(value):
    try:
        uuid.UUID(value)
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
        if "Dataset" in node_labels:
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


# should I check the edges?
def extract_dataset_from_AP(ap_payload: Dict[str, Any]) -> Dict[str, Any]:
    valid_processes = {"register", "update", "load"}

    try:
        query_data = APRequest.model_validate(ap_payload)
        G = json_to_graph(query_data)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )
    AP_nodes = []
    for node_id, attributes in G.nodes(data=True):
        if "Analytical_Pattern" in attributes.get("labels", []):
            AP_nodes.append(node_id)

    operator_nodes = []
    for node_id, attributes in G.nodes(data=True):
        if "DataModelManagement_Operator" in attributes.get("labels", []):
            operator_nodes.append(node_id)

    dataset_nodes = []
    for node_id, attributes in G.nodes(data=True):
        if "Dataset" in attributes.get("labels", []):
            dataset_nodes.append(node_id)

    user_nodes = []
    for node_id, attributes in G.nodes(data=True):
        if "User" in attributes.get("labels", []):
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
    if not is_valid_uuid_strict(dataset_id):
        new_dataset_id = str(uuid.uuid4())
        G = nx.relabel_nodes(G, {dataset_id: new_dataset_id})
        dataset_id = new_dataset_id

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if AP_process not in valid_processes:
        valid_list = ", ".join(sorted(valid_processes))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The Analytical Pattern 'Process' must be one of {valid_list}, but found '{AP_process}'.",
        )

    dataset_properties = G.nodes[dataset_id].get("properties", {}).copy()
    ordered_dataset = {
        "@context": {**CONTEXT_TEMPLATE, **REFERENCES_TEMPLATE},
        "@id": dataset_id,
    }
    ordered_dataset.update(dataset_properties)

    return json.dumps(ordered_dataset, indent=2)
