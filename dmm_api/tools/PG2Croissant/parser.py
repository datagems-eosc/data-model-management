from typing import List
import logging
from dmm_api.tools.PG2Croissant.model import (
    Dataset,
    FileObject,
    RecordSet,
    Field,
    ColumnStatistics,
)

logger = logging.getLogger(__name__)


def parse_heavyProfile(pgjson: dict) -> List[Dataset]:
    """Parse heavy profile with optimized indexing"""
    logger.info("Starting parse_heavyProfile")

    # Build indexes for O(1) lookups
    node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}
    edge_index = _build_edge_index(pgjson.get("edges", []))

    datasets = []
    for node in pgjson.get("nodes", []):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            logger.debug(f"Processing dataset: {dataset_id}")

            distribution = extract_distributions(
                dataset_id, pgjson=pgjson, node_index=node_index, edge_index=edge_index
            )
            recordSets = extract_recordSets(
                dataset_id, pgjson=pgjson, node_index=node_index, edge_index=edge_index
            )

            dataset = Dataset(
                id=dataset_id,
                distribution=distribution,
                recordSet=recordSets,
                properties=dataset_properties,
            )
            datasets.append(dataset)

    logger.info(f"parse_heavyProfile completed: found {len(datasets)} datasets")
    return datasets


def _build_edge_index(edges: list) -> dict:
    """Build index mapping 'from' node IDs to their outgoing edges"""
    edge_index = {}
    for edge in edges:
        from_id = edge.get("from")
        if from_id:
            if from_id not in edge_index:
                edge_index[from_id] = []
            edge_index[from_id].append(edge)
    return edge_index


def parse_lightProfile(pgjson: dict) -> List[Dataset]:
    """Parse light profile with optimized indexing"""
    logger.info("Starting parse_lightProfile")

    # Build indexes for O(1) lookups
    node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}
    edge_index = _build_edge_index(pgjson.get("edges", []))

    datasets = []
    for node in pgjson.get("nodes", []):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            logger.debug(f"Processing dataset: {dataset_id}")

            distribution = extract_distributions(
                dataset_id, pgjson=pgjson, node_index=node_index, edge_index=edge_index
            )
            dataset = Dataset(
                id=dataset_id,
                distribution=distribution,
                recordSet=[],
                properties=dataset_properties,
            )
            datasets.append(dataset)

    logger.info(f"parse_lightProfile completed: found {len(datasets)} datasets")
    return datasets


def parse_dataset(pgjson: dict) -> List[Dataset]:
    datasets = []
    for node in pgjson.get("nodes", []):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            dataset = Dataset(
                id=dataset_id,
                distribution=[],
                recordSet=[],
                properties=dataset_properties,
            )
            datasets.append(dataset)
    return datasets


def extract_fields(
    recordSet_id: str, pgjson: dict, node_index: dict = None, edge_index: dict = None
) -> List[Field]:
    """Extract fields for a recordSet using indexed lookups"""
    if node_index is None:
        node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}
    if edge_index is None:
        edge_index = _build_edge_index(pgjson.get("edges", []))

    fields = []
    # Get edges from this recordSet
    for edge in edge_index.get(recordSet_id, []):
        if "field" in edge.get("labels", []):
            field_id = edge.get("to")
            field_node = node_index.get(field_id)
            if not field_node:
                logger.warning(f"Field node not found: {field_id}")
                continue

            field_properties = field_node.get("properties", {}).copy()
            statistics = extract_columnStatistics(
                field_id=field_id, node_index=node_index, edge_index=edge_index
            )
            fileObject_id = extract_source(field_id=field_id, edge_index=edge_index)

            source = {
                "extract": {"column": field_node.get("properties", {}).get("name", "")},
                "fileObject": {"@id": fileObject_id},
            }
            field_properties["source"] = source

            field = Field(
                id=field_id,
                statistics=statistics,
                properties=field_properties if field_properties else {},
            )
            fields.append(field)

    logger.debug(f"Extracted {len(fields)} fields for recordSet {recordSet_id}")
    return fields


def extract_columnStatistics(
    field_id: str, pgjson: dict = None, node_index: dict = None, edge_index: dict = None
) -> List[ColumnStatistics]:
    """Extract column statistics for a field using indexed lookups"""
    if edge_index is None:
        edge_index = _build_edge_index(pgjson.get("edges", []))
    if node_index is None:
        node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}

    columnStatistics = []
    # Get edges from this field
    for edge in edge_index.get(field_id, []):
        if "statistics" in edge.get("labels", []):
            columnStatistics_id = edge.get("to")
            columnStatistics_node = node_index.get(columnStatistics_id)
            if columnStatistics_node:
                columnStatistic = ColumnStatistics(
                    id=columnStatistics_id,
                    properties=columnStatistics_node.get("properties", {}),
                )
                columnStatistics.append(columnStatistic)

    return columnStatistics


def extract_recordSets(
    dataset_id: str, pgjson: dict, node_index: dict = None, edge_index: dict = None
) -> List[RecordSet]:
    """Extract recordSets for a dataset using indexed lookups"""
    if node_index is None:
        node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}
    if edge_index is None:
        edge_index = _build_edge_index(pgjson.get("edges", []))

    recordSets = []
    seen_ids = set()

    # Get edges from this dataset
    for edge in edge_index.get(dataset_id, []):
        if "recordSet" in edge.get("labels", []):
            recordSet_id = edge.get("to")
            # Check if this recordSet isn't already in the list (because of duplicate edges)
            if recordSet_id not in seen_ids:
                recordSet_node = node_index.get(recordSet_id)
                if recordSet_node:
                    fields = extract_fields(
                        recordSet_id,
                        pgjson,
                        node_index=node_index,
                        edge_index=edge_index,
                    )
                    recordSet = RecordSet(
                        id=recordSet_id,
                        fields=fields,
                        properties=recordSet_node.get("properties", {}),
                    )
                    recordSets.append(recordSet)
                    seen_ids.add(recordSet_id)

    logger.debug(f"Extracted {len(recordSets)} recordSets for dataset {dataset_id}")
    return recordSets


def extract_distributions(
    dataset_id: str, pgjson: dict, node_index: dict = None, edge_index: dict = None
) -> List[FileObject]:
    """Extract distributions for a dataset using indexed lookups"""
    if node_index is None:
        node_index = {node.get("id"): node for node in pgjson.get("nodes", [])}
    if edge_index is None:
        edge_index = _build_edge_index(pgjson.get("edges", []))

    distributions = []
    # Get edges from this dataset
    for edge in edge_index.get(dataset_id, []):
        if "distribution" in edge.get("labels", []):
            fileObject_id = edge.get("to")
            fileObject_node = node_index.get(fileObject_id)
            if fileObject_node:
                fileObject_properties = fileObject_node.get("properties", {}).copy()

                # Look for containedIn edge
                for contained_edge in edge_index.get(fileObject_id, []):
                    if "containedIn" in contained_edge.get("labels", []):
                        fileObject_properties["containedIn"] = {
                            "@id": contained_edge.get("to")
                        }

                fileObject = FileObject(
                    id=fileObject_id,
                    properties=fileObject_properties if fileObject_properties else {},
                )
                distributions.append(fileObject)

    logger.debug(
        f"Extracted {len(distributions)} distributions for dataset {dataset_id}"
    )
    return distributions


def extract_source(field_id: str, pgjson: dict = None, edge_index: dict = None) -> str:
    """Extract source fileObject ID for a field using indexed lookup"""
    if edge_index is None:
        edge_index = _build_edge_index(pgjson.get("edges", []))

    fileObject_id = None
    # Get edges from this field
    for edge in edge_index.get(field_id, []):
        labels = edge.get("labels", [])
        # Accept variations: source/fileObject, source_fileObject, source___fileObject, etc.
        if any(
            label in ["source/fileObject", "source_fileObject", "source___fileObject"]
            for label in labels
        ):
            fileObject_id = edge.get("to")
            break

    return fileObject_id
