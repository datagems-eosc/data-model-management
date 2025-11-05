from dmm_api.tools.AP.parse_AP import APRequest


def update_dataset_id(
    ap_payload: APRequest, old_field: str, new_field: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(old_field) and "Dataset" in node.labels:
            node.id = new_field

    for edge in ap_payload.edges:
        if str(edge.target) == str(old_field):
            edge.target = new_field
        if str(edge.source) == str(old_field):
            edge.source = new_field

    return ap_payload.model_dump(by_alias=True, exclude_defaults=True)


def update_dataset_archivedAt(
    ap_payload: APRequest, dataset_id: str, new_path: str
) -> APRequest:
    for node in ap_payload.nodes:
        if str(node.id) == str(dataset_id) and "Dataset" in node.labels:
            if "properties" not in node.model_dump():
                node.properties = {}
            node.properties["archivedAt"] = new_path
    return ap_payload.model_dump(by_alias=True, exclude_defaults=True)
