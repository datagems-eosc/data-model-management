
import os
from typing import Optional, List
import requests
import structlog

from dmm_api.tools.AP.parse_AP import APRequest, json_to_graph

logger = structlog.get_logger(__name__)

GRAFEO_URL = os.getenv("GRAFEO_URL", "http://localhost:7474")


## The generated Grafeo queries will first check if the node id already exists, and if so, it will update the properties of the existing node instead of creating a new one. 
# This is achieved using the MERGE clause in the generated Cypher queries, which ensures that nodes with the same id are not duplicated but rather updated with new properties if they already exist.
def AP_to_Grafeo(AP_payload: APRequest) -> dict:
    G = json_to_graph(AP_payload)
    grafeo_queries = []
    grafeo_json = {
        "nodes": [
            {
                "id": node_id,
                "labels": G.nodes[node_id].get("labels", []),
                "properties": G.nodes[node_id].get("properties", {}),
            }
            for node_id in G.nodes()
        ],
        "edges": [
            {"from": u, "to": v, "labels": data.get("labels", []), "properties": data.get("properties", {})}
            for u, v, data in G.edges(data=True)
        ],
    }

    # Get AP id from the node with label "Analytical_Pattern" and property "id"
    ap_node = next((node for node in grafeo_json["nodes"] if "Analytical_Pattern" in node["labels"]), None)
    task_node = next((node for node in grafeo_json["nodes"] if "Task" in node["labels"]), None)
    operator_node = next((node for node in grafeo_json["nodes"] if "Operator" in node["labels"]), None)
    if not ap_node:
        raise ValueError("No AP node with id property found in the graph.")
    
    check_queries = {}
    check_queries["Analytical_Pattern"] = f"MATCH (n:Analytical_Pattern {{id: '{ap_node['id']}'}}) RETURN n"
    check_queries["Task"] = f"MATCH (n:Task {{id: '{task_node['id']}'}}) RETURN n"
    check_queries["Operator"] = f"MATCH (n:Operator) WHERE n.id IN {tuple(op['id'] for op in operator_node)} RETURN n"

    for node in grafeo_json["nodes"]:
        # Normalize labels
        labels = [normalize_label(l) for l in node.get("labels", [])]
        label_str = " :" + " :".join(labels) if labels else ""

        # Build properties
        props = {normalize_label(k): v for k, v in node.get("properties", {}).items()}
        props_str = ", ".join(f'{k}: "{v}"' for k, v in props.items())

        grafeo_queries.append(
            f'''MERGE (n{label_str} {{id: "{node.get("id")}"}})
            SET n += {{{props_str}}}
            ON CREATE SET n.created_at = timestamp(), n.updated_at = timestamp()
            ON MATCH SET n.updated_at = timestamp()'''
        )

    for edge in grafeo_json["edges"]:
        from_id = edge["from"]
        to_id = edge["to"]
        props = {normalize_label(k): v for k, v in edge.get("properties", {}).items()}
        set_clause = ", ".join(f'r.{k} = "{v}"' for k, v in props.items())
        for label in edge.get("labels", []):
            q = f'''
                MATCH (a {{id: "{from_id}"}}), (b {{id: "{to_id}"}})
                MERGE (a)-[r:{normalize_label(label)}]->(b)
            '''
            if set_clause:
                q += f" SET {set_clause}"
            grafeo_queries.append(q)
 
    return check_queries, grafeo_queries

def normalize_label(label: str) -> str:
    return label.replace(":", "__").replace(" ", "_")

# TODO: Implement the function to convert Grafeo response back to APRequest object, which can be used for testing the AP search endpoint. This will involve mapping the structure of the Grafeo response to the APRequest schema, ensuring that all necessary fields are correctly extracted and formatted.
def Grafeo_to_AP(raw_data: dict, property_filter: Optional[List[str]] = None) -> dict:
    """
    Convert the raw Grafeo response (nodes/edges keyed by internal _id)
    into the desired format: lists of nodes and edges with user-facing IDs.
    """
    nodes_by_internal = raw_data.get("aps", {}).get("nodes", {})
    edges_by_internal = raw_data.get("aps", {}).get("edges", {})

    # Step 1: map internal _id -> user-facing "id" (UUID)
    internal_to_user_id = {}
    for internal_id, node in nodes_by_internal.items():
        user_id = node.get("id")  # the UUID property
        if user_id:
            internal_to_user_id[internal_id] = user_id

    # Step 2: build nodes list
    nodes_list = []
    for internal_id, node in nodes_by_internal.items():
        labels = node.get("_labels", [])
        for i, label in enumerate(labels):
            labels[i] = label.replace("__", ":")
        user_id = node.get("id")
        if not user_id:
            continue

        # Extract properties: everything except _id, _labels, and the 'id' field itself
        if property_filter:
            properties = {k.replace("__", ":"): v for k, v in node.items() if k not in ("_id", "_labels", "id") and k in property_filter}
        else:
            properties = {k.replace("__", ":"): v for k, v in node.items() if k not in ("_id", "_labels", "id")}

        node_entry = {
            "labels": labels,
            "id": user_id
        }
        if properties:
            node_entry["properties"] = properties
        nodes_list.append(node_entry)

    # Step 3: build edges list
    edges_list = []
    for internal_id, edge in edges_by_internal.items():
        source_internal = edge.get("_source")
        target_internal = edge.get("_target")
        rel_type = edge.get("_type").replace("__", ":") if edge.get("_type") else None
        if not (source_internal and target_internal and rel_type):
            continue

        from_id = internal_to_user_id.get(source_internal)
        to_id = internal_to_user_id.get(target_internal)
        if not (from_id and to_id):
            continue   # skip if node not found (should not happen)

        # Edge properties (if any) – ignore internal fields
        edge_props = {k: v for k, v in edge.items() if k not in ("_id", "_source", "_target", "_type")}

        edge_entry = {
            "from": from_id,
            "to": to_id,
            "labels": [rel_type]   # as an array, like in example
        }
        if edge_props:
            edge_entry["properties"] = edge_props
        edges_list.append(edge_entry)

    return {
        "ap": {
            "nodes": nodes_list,
            "edges": edges_list
        }
    }

def grafeo_begin():
    resp = requests.post(f"{GRAFEO_URL}/transaction/start")
    resp.raise_for_status()
    return resp.json()["txId"]

def grafeo_execute(txId, query):
    resp = requests.post(
        f"{GRAFEO_URL}/transaction/{txId}/execute",
        json={"query": query}
    )
    resp.raise_for_status()
    return resp.json()

def grafeo_commit(txId):
    resp = requests.post(f"{GRAFEO_URL}/transaction/{txId}/commit")
    resp.raise_for_status()

def grafeo_rollback(txId):
    requests.post(f"{GRAFEO_URL}/transaction/{txId}/rollback")

def store_AP_in_grafeo(ap: APRequest):
    check_queries, grafeo_queries = AP_to_Grafeo(ap)
    try:

        ## Add something to retrieve ap id / task id / operator id 

        txId = grafeo_begin()
        
        for node, query in check_queries.items():
            response = grafeo_execute(txId, query)
            rows = response.get("rows", [])
            if rows:
                grafeo_rollback(txId)
                raise ValueError(f"AP with node {node} already exists in Grafeo. Rolling back transaction.")
        for query in grafeo_queries:
            grafeo_execute(txId, query)
        grafeo_commit(txId)

    except Exception as e:
        grafeo_rollback(txId)
        raise 
        
