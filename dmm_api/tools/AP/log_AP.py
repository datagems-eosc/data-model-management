
import os
from datetime import datetime, timezone
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
    operator_nodes = [
        node
        for node in grafeo_json["nodes"]
        if any(label == "Operator" or label.endswith("_Operator") for label in node.get("labels", []))
    ]
    if not ap_node:
        raise ValueError("No AP node with id property found in the graph.")
    if not task_node:
        raise ValueError("No Task node with id property found in the graph.")
    
    check_queries = {}
    check_queries["Analytical_Pattern"] = { "query":
        f"MATCH (n:Analytical_Pattern {{id: \"{escape_cypher_string(ap_node['id'])}\"}}) RETURN n", 
        "id": ap_node["id"]}
    
    check_queries["Task"] = { "query":
        f"MATCH (n:Task {{id: \"{escape_cypher_string(task_node['id'])}\"}}) RETURN n",
        "id": task_node["id"]
    }
    operator_ids = [node["id"] for node in operator_nodes if node.get("id")]
    if operator_ids:
        operator_ids_cypher = ", ".join(
            f'"{escape_cypher_string(operator_id)}"' for operator_id in operator_ids
        )
        check_queries["Operator"] = {
            "query": (
                "MATCH (n) "
                f"WHERE n.id IN [{operator_ids_cypher}] "
                "AND ANY(label IN labels(n) WHERE label = 'Operator' OR label ENDS WITH '_Operator') "
                "RETURN n"
            ),
            "id": operator_ids
        }

    for node in grafeo_json["nodes"]:
        # Normalize labels
        labels = [normalize_label(l) for l in node.get("labels", [])]
        label_str = " :" + " :".join(labels) if labels else ""

        # Build properties
        props = {normalize_label(k): v for k, v in node.get("properties", {}).items()}

        node_id_escaped = escape_cypher_string(node.get("id"))
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        merge_clause = f'MERGE (n{label_str} {{id: "{node_id_escaped}"}})'
        # Use individual SET assignments — Grafeo does not support SET n += {map}
        individual_sets = ", ".join(
            f'n.{k} = "{escape_cypher_string(v)}"' for k, v in props.items()
        )
        set_props = f" SET {individual_sets}" if individual_sets else ""
        on_create = f' ON CREATE SET n.created_at = "{now_iso}", n.updated_at = "{now_iso}"'
        on_match = f' ON MATCH SET n.updated_at = "{now_iso}"'
        grafeo_queries.append(merge_clause + on_create + on_match + set_props)

    for edge in grafeo_json["edges"]:
        from_id = edge["from"]
        to_id = edge["to"]
        props = {normalize_label(k): v for k, v in edge.get("properties", {}).items()}
        set_clause = ", ".join(
            f'r.{k} = "{escape_cypher_string(v)}"' for k, v in props.items()
        )
        for label in edge.get("labels", []):
            q = f'''
                MATCH (a {{id: "{escape_cypher_string(from_id)}"}}), (b {{id: "{escape_cypher_string(to_id)}"}})
                MERGE (a)-[r:{normalize_label(label)}]->(b)
            '''
            if set_clause:
                q += f" SET {set_clause}"
            grafeo_queries.append(q)
    return check_queries, grafeo_queries

def normalize_label(label: str) -> str:
    return label.replace(":", "__").replace(" ", "_")


def escape_cypher_string(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")

# TODO: Implement the function to convert Grafeo response back to APRequest object, which can be used for testing the AP search endpoint. This will involve mapping the structure of the Grafeo response to the APRequest schema, ensuring that all necessary fields are correctly extracted and formatted.
def Grafeo_to_AP(raw_data: dict) -> dict:
    """
    Convert the raw Grafeo response (nodes/edges keyed by internal _id)
    into the desired format: lists of nodes and edges with user-facing IDs.
    """
    nodes_by_internal = raw_data.get("ap", {}).get("nodes", {})
    edges_by_internal = raw_data.get("ap", {}).get("edges", {})

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
    if resp.status_code == 404:
        # Some Grafeo deployments expose only /cypher and not transaction APIs.
        logger.warning("Grafeo transaction API not available; falling back to /cypher mode")
        return None
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("txId")

def grafeo_execute(txId, query):
    if txId is None:
        resp = requests.post(
            f"{GRAFEO_URL}/cypher",
            json={"query": query}
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise requests.HTTPError(f"{e}. Grafeo response: {resp.text}") from e
        return resp.json()

    resp = requests.post(
        f"{GRAFEO_URL}/transaction/{txId}/execute",
        json={"query": query}
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(f"{e}. Grafeo response: {resp.text}") from e
    return resp.json()

def grafeo_commit(txId):
    if txId is None:
        return
    resp = requests.post(f"{GRAFEO_URL}/transaction/{txId}/commit")
    resp.raise_for_status()

def grafeo_rollback(txId):
    if txId is None:
        return
    requests.post(f"{GRAFEO_URL}/transaction/{txId}/rollback")

def store_AP_in_grafeo(ap: APRequest):
    txId = None
    try:
        check_queries, grafeo_queries = AP_to_Grafeo(ap)

        ## Add something to retrieve ap id / task id / operator id 

        txId = grafeo_begin()
        
        for node, content in check_queries.items():
            try:
                response = grafeo_execute(txId, content["query"])
            except Exception as e:
                raise ValueError(f"Grafeo duplicate-check failed for {node}. Query: {content['query']}. Error: {e}") from e
            rows = response.get("rows", [])
            if rows:
                grafeo_rollback(txId)
                raise ValueError(f"AP already exist: Node {node} with uuid {content['id']} already exists in Grafeo. Rolling back transaction.")
        for idx, query in enumerate(grafeo_queries, start=1):
            try:
                grafeo_execute(txId, query)
            except Exception as e:
                raise ValueError(f"Grafeo write query #{idx} failed. Query: {query}. Error: {e}") from e
        grafeo_commit(txId)

    except Exception:
        if txId is not None:
            grafeo_rollback(txId)
        raise 
        

def Grafeo_to_AP_node(grafeo_node: dict) -> dict:
    labels = grafeo_node.get("_labels", [])
    for i, label in enumerate(labels):
        labels[i] = label.replace("__", ":")
    node_id = grafeo_node.get("id")
    if not node_id:
        raise ValueError("Node is missing 'id' property")

    properties = {k.replace("__", ":"): v for k, v in grafeo_node.items() if k not in ("_id", "_labels", "id")}

    node_entry = {
        "labels": labels,
        "id": node_id
    }
    if properties:
        node_entry["properties"] = properties
    return node_entry