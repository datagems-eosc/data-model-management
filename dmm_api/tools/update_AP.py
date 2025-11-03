from dmm_api.tools.parse_AP import APRequest


def update_dataset_field(
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
