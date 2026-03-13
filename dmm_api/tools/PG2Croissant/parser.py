from typing import List
from dmm_api.tools.PG2Croissant.model import Dataset, FileObject, RecordSet, Field, ColumnStatistics, Source 

def parse_heavyProfile(pgjson: dict) -> List[Dataset]:
    datasets = []
    for node in pgjson.get("nodes", {}):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            distribution = extract_distributions(dataset_id, pgjson=pgjson)
            recordSets = extract_recordSets(dataset_id, pgjson=pgjson)
            dataset = Dataset(id=dataset_id, distribution=distribution, recordSet=recordSets, properties=dataset_properties)
            datasets.append(dataset)
    return datasets

def parse_lightProfile(pgjson: dict) -> List[Dataset]: 
    datasets = []
    for node in pgjson.get("nodes", {}):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            distribution = extract_distributions(dataset_id, pgjson=pgjson)
            dataset = Dataset(id=dataset_id, distribution=distribution, recordSet=[], properties=dataset_properties)
            datasets.append(dataset)
    return datasets

def parse_dataset(pgjson: dict) -> List[Dataset]:
    datasets = []
    for node in pgjson.get("nodes", {}):
        if node.get("properties", {}).get("type") == "sc:Dataset":
            dataset_id = node.get("id")
            dataset_properties = node.get("properties", {})
            dataset = Dataset(id=dataset_id, distribution=[], recordSet=[], properties=dataset_properties)
            datasets.append(dataset)
    return datasets

def extract_fields(recordSet_id: str, pgjson: dict) -> List[Field]:
    fields = []
    for edge in pgjson.get("edges", []):
        if edge.get("from",{}) == recordSet_id and "field" in edge.get("labels", {}):
            field_id = edge.get("to")
            field_nodes = [item for item in pgjson.get("nodes", []) if item.get("id") == field_id]
            field_properties = field_nodes[0].get("properties", {})
            statistics = extract_columnStatistics(field_id=field_id, pgjson=pgjson)
            fileObject_id = extract_source(field_id=field_id, pgjson=pgjson)
            source = {
                "extract": {"column": field_nodes[0].get("properties", {}).get("name", "")},
                "fileObject": {"@id": fileObject_id}
            }
            field_properties["source"] = source

            field = Field(id=field_id, statistics = statistics, properties=field_properties if field_properties else {})
            fields.append(field)
    return fields

def extract_columnStatistics(field_id: str, pgjson: dict) -> List[ColumnStatistics]:
    columnStatistics = []
    for edge in pgjson.get("edges", []):
        if edge.get("from",{}) == field_id and "statistics" in edge.get("labels", {}):
            columnStatistics_id = edge.get("to")
            columnStatistics_properties = [item for item in pgjson.get("nodes", []) if item.get("id") == columnStatistics_id]
            columnStatistic = ColumnStatistics(id=columnStatistics_id, properties=columnStatistics_properties[0].get("properties", {}) if columnStatistics_properties else {})
            columnStatistics.append(columnStatistic)
    return columnStatistics

def extract_recordSets(dataset_id: str, pgjson: dict) -> List[RecordSet]:
    recordSets = []
    for edge in pgjson.get("edges", []):
        if edge.get("from",{}) == dataset_id and "recordSet" in edge.get("labels", {}):
            recordSet_id = edge.get("to")
            # Check if this recordSet isn't already in the list (because of the duplicate recordSet edges)
            if not any(rs.id == recordSet_id for rs in recordSets):
                recordSet_properties = [item for item in pgjson.get("nodes", []) if item.get("id") == recordSet_id]
                fields = extract_fields(recordSet_id, pgjson)
                recordSet = RecordSet(id=recordSet_id, fields=fields, properties=recordSet_properties[0].get("properties", {}) if recordSet_properties else {})
                recordSets.append(recordSet)
    return recordSets

def extract_distributions(dataset_id: str, pgjson: dict) -> List[FileObject]:
    distributions = []
    for edge in pgjson.get("edges", []):
        if edge.get("from",{}) == dataset_id and "distribution" in edge.get("labels", {}):
            fileObject_id = edge.get("to")
            fileObject_properties = [item for item in pgjson.get("nodes", []) if item.get("id") == fileObject_id]
            fileObject_properties = fileObject_properties[0].get("properties", {}) if fileObject_properties else {}
            for edge in pgjson.get("edges", []):
                if edge.get("from",{}) == fileObject_id and "containedIn" in edge.get("labels", {}):
                    fileObject_properties["containedIn"] = {"@id": edge.get("to")}
            fileObject = FileObject(id=fileObject_id, properties=fileObject_properties if fileObject_properties else {})
            distributions.append(fileObject)
    return distributions

def extract_source(field_id: str, pgjson: dict) -> str:    
    fileObject_id = None
    for edge in pgjson.get("edges", []):
        labels = edge.get("labels", [])
        # Accept variations: source/fileObject, source_fileObject, source___fileObject, etc.
        if edge.get("from",{}) == field_id and any(
            label in ["source/fileObject", "source_fileObject", "source___fileObject"] 
            for label in labels
        ):
            fileObject_id = edge.get("to")
    return fileObject_id
    

