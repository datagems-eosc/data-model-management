import uuid
from datetime import datetime
import networkx as nx
from dmm_api.tools.AP.parse_AP import APRequest, json_to_graph


def generate_update_AP(ap_payload: APRequest, new_path: str) -> APRequest:
    G_load = json_to_graph(ap_payload)

    user_node = None
    dataset_node = None

    for node_id, attrs in G_load.nodes(data=True):
        if "User" in attrs.get("labels", []):
            user_node = (node_id, attrs)
        elif "Dataset" in attrs.get("labels", []):
            dataset_node = (node_id, attrs)

    if not user_node or not dataset_node:
        raise ValueError("Required user or dataset information not found in AP payload")

    dataset_node[1]["properties"]["archivedAt"] = new_path

    ap_id = str(uuid.uuid4())
    operator_id = str(uuid.uuid4())
    task_id = str(uuid.uuid4())

    G_update = nx.MultiDiGraph()

    G_update.add_node(
        ap_id,
        labels=["Analytical_Pattern"],
        properties={
            "Description": "Analytical Pattern to update a dataset",
            "Name": "Update Dataset AP",
            "Process": "update",
            "PublishedDate": datetime.now().strftime("%Y-%m-%d"),
            "StartTime": datetime.now().strftime("%H:%M:%S"),
        },
    )

    G_update.add_node(
        operator_id,
        labels=["DataModelManagement_Operator"],
        properties={
            "Description": "An operator to update a dataset into DataGEMS",
            "Name": "Update Operator",
            "Parameters": {"command": "update"},
            "PublishedDate": datetime.now().strftime("%Y-%m-%d"),
            "Software": {},
            "StartTime": datetime.now().strftime("%H:%M:%S"),
            "Step": 1,
        },
    )

    G_update.add_node(dataset_node[0], **dataset_node[1])
    G_update.add_node(user_node[0], **user_node[1])

    G_update.add_node(
        task_id,
        labels=["Task"],
        properties={
            "Description": "Task to update a dataset",
            "Name": "Dataset Updating Task",
        },
    )

    edges = [
        (ap_id, operator_id, {"labels": ["consist_of"]}),
        (operator_id, dataset_node[0], {"labels": ["input"]}),
        (user_node[0], operator_id, {"labels": ["intervene"]}),
        (task_id, ap_id, {"labels": ["is_achieved"]}),
        (user_node[0], task_id, {"labels": ["request"]}),
    ]

    G_update.add_edges_from(edges)

    update_json = {
        "nodes": [
            {
                "@id": node_id,
                "labels": G_update.nodes[node_id]["labels"],
                "properties": G_update.nodes[node_id]["properties"],
            }
            for node_id in G_update.nodes()
        ],
        "edges": [
            {"from": u, "to": v, "labels": data["labels"]}
            for u, v, data in G_update.edges(data=True)
        ],
    }

    return update_json
