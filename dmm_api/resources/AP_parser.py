import networkx as nx
from fastapi import HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Dict, Any


class Node(BaseModel):
    id: str | int
    labels: List[str]
    properties: Dict[str, Any]


class Edge(BaseModel):
    source: str | int = Field(..., alias="from")
    target: str | int = Field(..., alias="to")
    labels: List[str] = []


class QueryRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]
    directed: bool = True
    multigraph: bool = True
    graph: Dict[str, Any] = {}


def json_to_graph(query_data: QueryRequest) -> nx.DiGraph | nx.MultiDiGraph:
    graph_data = query_data.model_dump(by_alias=True)
    G = nx.node_link_graph(
        graph_data, nodes="nodes", edges="edges", source="from", target="to"
    )
    return G


# Simplified case with only one SQL_Operator, Dataset and FileObject in the AP
def extract_from_AP(query_data: QueryRequest):
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
