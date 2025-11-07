import uuid
import copy
from dmm_api.tools.AP.parse_AP import APRequest, Edge, Node


def update_dataset_id(
    ap_payload: APRequest, old_field: str, new_field: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(old_field) and "sc:Dataset" in node.labels:
            node.id = new_field

    for edge in ap_payload.edges:
        if str(edge.target) == str(old_field):
            edge.target = new_field
        if str(edge.source) == str(old_field):
            edge.source = new_field

    return ap_payload


def update_dataset_archivedAt(
    ap_payload: APRequest, dataset_id: str, new_path: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(dataset_id) and "sc:Dataset" in node.labels:
            if "properties" not in node.model_dump():
                node.properties = {}
            node.properties["archivedAt"] = new_path
    return ap_payload


def update_after_query(
    ap_payload: APRequest, dataset_id: str, new_path: str
) -> APRequest:
    updated_AP = copy.deepcopy(ap_payload)

    output_edge = next((e for e in updated_AP.edges if "output" in e.labels), None)
    if not output_edge:
        raise ValueError("No edge with label 'output' found.")

    old_dataset_id = output_edge.target
    s3_path = new_path.replace("/s3/", "s3://")
    updated_AP = update_dataset_id(updated_AP, old_dataset_id, dataset_id)
    updated_AP = update_dataset_archivedAt(updated_AP, dataset_id, s3_path)

    new_file_id = str(uuid.uuid4())

    new_file_node = Node(
        **{
            "@id": new_file_id,
            "labels": ["cr:FileObject", "CSV"],
            "properties": {
                "@type": "cr:FileObject",
                "contentSize": "1000000 B",
                "contentUrl": f"{s3_path}/output.csv",
                "description": "Output file generated from query",
                "encodingFormat": "text/csv",
                "name": "output.csv",
                "sha256": "hash1234567890abcdef",
            },
        }
    )
    updated_AP.nodes.append(new_file_node)

    new_edge = Edge(
        **{"from": dataset_id, "to": new_file_id, "labels": ["distribution"]}
    )

    updated_AP.edges.append(new_edge)

    return updated_AP
