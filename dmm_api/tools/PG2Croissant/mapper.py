from dmm_api.constants import CROISSANT_CONTEXT


def map_to_croissant(datasets):
    for dataset in datasets:
        distribution = []
        recordSets = []
        for fileObject in dataset.distribution:
            distribution.append(map_fileObjects(fileObject))
        for recordSet in dataset.recordSet:
            recordSets.append(map_recordSet(recordSet))

        dataset_dict = {
            "@context": CROISSANT_CONTEXT,
            "@type": "Dataset",
            "@id": dataset.id,
            "distribution": distribution,
            "recordSet": recordSets,
        }
        for key, val in dataset.properties.items():
            if key == "type":
                dataset_dict["@type"] = val
            elif key == "id":
                dataset_dict["@id"] = val
            else:
                dataset_dict[key] = val
        if dataset_dict.get("@type") is None:
            dataset_dict["@type"] = "cr:Dataset"
    return dataset_dict


def map_to_croissant_dataset(datasets):
    for dataset in datasets:
        dataset_dict = {"@context": CROISSANT_CONTEXT, "@id": dataset.id}
        for key, val in dataset.properties.items():
            if key == "type":
                dataset_dict["@type"] = val
            elif key == "id":
                dataset_dict["@id"] = val
            else:
                dataset_dict[key] = val
        if dataset_dict.get("@type") is None:
            dataset_dict["@type"] = "cr:Dataset"
    return dataset_dict


def map_fileObjects(fileObject):
    fileObject_dict = {"@id": fileObject.id}
    for key, val in fileObject.properties.items():
        if key == "type":
            fileObject_dict["@type"] = val
        elif key == "id":
            fileObject_dict["@id"] = val
        else:
            fileObject_dict[key] = val
    if fileObject_dict.get("@type") is None:
        fileObject_dict["@type"] = "cr:FileObject"
    return fileObject_dict


def map_recordSet(recordSet):
    fields = []
    for field in recordSet.fields:
        fields.append(map_field(field))

    recordSet_dict = {"@id": recordSet.id}
    for key, val in recordSet.properties.items():
        if key == "type":
            recordSet_dict["@type"] = val
        elif key == "id":
            recordSet_dict["@id"] = val
        else:
            recordSet_dict[key] = val
    recordSet_dict["field"] = fields
    if recordSet_dict.get("@type") is None:
        recordSet_dict["@type"] = "cr:RecordSet"
    return recordSet_dict


def map_field(field):
    statistics = []
    for statistic in field.statistics:
        statistics.append(map_statistics(statistic))

    field_dict = {"@id": field.id}
    for key, val in field.properties.items():
        if key == "type":
            field_dict["@type"] = val
        elif key == "id":
            field_dict["@id"] = val
        else:
            field_dict[key] = val

    field_dict["statistics"] = statistics[0] if statistics else None
    if field_dict.get("@type") is None:
        field_dict["@type"] = "cr:Field"
    return field_dict


def map_statistics(statistic):
    statistic_dict = {"@id": statistic.id, "@type": "dg:ColumnStatistics"}
    for key, val in statistic.properties.items():
        if key == "type":
            statistic_dict["@type"] = val
        elif key == "id":
            statistic_dict["@id"] = val
        else:
            statistic_dict[key] = val
    if statistic_dict.get("@type") is None:
        statistic_dict["@type"] = "dg:ColumnStatistics"
    return statistic_dict
