import uuid
import copy
from dmm_api.tools.AP.parse_AP import APRequest, Edge, Node


# TODO: update the id for more than one dataset?
# def update_dataset_id(
#     ap_payload: APRequest, old_field: str, new_field: str
# ) -> APRequest:
#     for node in ap_payload.nodes:
#         if str(node.id) == str(old_field) and "sc:Dataset" in node.labels:
#             node.id = new_field

#     for edge in ap_payload.edges:
#         if str(edge.target) == str(old_field):
#             edge.target = new_field
#         if str(edge.source) == str(old_field):
#             edge.source = new_field

#     return ap_payload

def update_fileObject_id(
    ap_payload: APRequest, old_field: str, new_field: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(old_field) and "cr:FileObject" in node.labels:
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

def update_fileObject_properties(
    ap_payload: APRequest, fileObject_id: str, new_path: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(fileObject_id) and "cr:FileObject" in node.labels:
            if "properties" not in node.model_dump():
                node.properties = {}
            node.properties["contentUrl"] = f"s3://{new_path}/output.csv"
            node.properties["contentSize"] = "1000000 B"
            node.properties["encodingFormat"] = "text/csv"
            node.properties["name"] = "output.csv"
            node.properties["description"] = "Output file generated from query"
            node.properties["sha256"] = "hash1234567890abcdef"
    return ap_payload

# I want it to return APRequest and the new dataset id
def update_output_dataset_id(ap_payload: APRequest) -> tuple[APRequest, str]:
    output_edge = next((e for e in ap_payload.edges if "output" in e.labels), None)
    if not output_edge:
        raise ValueError("No edge with label 'output' found.")
    old_dataset_id = output_edge.target
    dataset_id = str(uuid.uuid4())
    updated_AP = update_dataset_id(ap_payload, old_dataset_id, dataset_id)
    return updated_AP, dataset_id

def generate_dataset_node(ap_payload: APRequest) -> tuple[APRequest, str]:
    dataset_id = str(uuid.uuid4())
    new_dataset_node = Node(
        **{
            "id": dataset_id,
            "labels": ["sc:Dataset"]
        }
    )
    ap_payload.nodes.append(new_dataset_node)
    return ap_payload, dataset_id

def update_AP_after_query(
    ap_payload: APRequest, dataset_id: str, new_path: str
) -> APRequest:
    updated_AP = copy.deepcopy(ap_payload)

    output_edge = next((e for e in updated_AP.edges if "output" in e.labels), None)
    if not output_edge:
        raise ValueError("No edge with label 'output' found.")

    old_fileObject_id = output_edge.target
    fileObject_id = str(uuid.uuid4())
    s3_path = new_path.replace("/s3/", "s3://")
    update_AP = update_fileObject_id(updated_AP, old_fileObject_id, fileObject_id)
    updated_AP = update_fileObject_properties(updated_AP,fileObject_id, new_path)

    updated_AP = update_dataset_archivedAt(updated_AP, dataset_id, s3_path)

    new_edge = Edge(
        **{"from": dataset_id, "to": fileObject_id, "labels": ["distribution"]}
    )

    updated_AP.edges.append(new_edge)

    return updated_AP

def add_sql_operators_to_ap(ap_payload: APRequest) -> APRequest:
    ap_payload = APRequest.model_validate(ap_payload)
    sql_operator_id = str(uuid.uuid4())
    new_dataset_node = Node(
        **{
            "id": sql_operator_id,
            "labels": ["Query_Operator", "SQL_Operator"]
        }
    )
    ap_payload.nodes.append(new_dataset_node)
    for node in ap_payload.nodes:
        if "NLQ_Operator" in node.labels:
            nlq_operator_id = node.id
            # The edge should be changed from SQL_Operator to NLQ_Operator
    if nlq_operator_id:
        new_edge = Edge(
                    **{"from": node.id, "to": sql_operator_id, "labels": ["follows"]}
                )
        ap_payload.edges.append(new_edge)
        for edge in ap_payload.edges:
            if str(edge.source) == str(nlq_operator_id) and "output" in edge.labels:
                edge.source = sql_operator_id

    return ap_payload
