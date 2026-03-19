from dmm_api.constants import CROISSANT_CONTEXT


def map_to_croissant_heavyProfile(datasets):
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
            else:
                dataset_dict[key] = val
    return dataset_dict


def map_to_croissant_lightProfile(datasets):
    for dataset in datasets:
        distribution = []
        for fileObject in dataset.distribution:
            distribution.append(map_fileObjects(fileObject))

        recordSet = []

        dataset_dict = {
            "@context": CROISSANT_CONTEXT,
            "@id": dataset.id,
            "distribution": distribution,
            "recordSet": recordSet,
        }
        for key, val in dataset.properties.items():
            if key == "type":
                dataset_dict["@type"] = val
            else:
                dataset_dict[key] = val
    return dataset_dict


def map_to_croissant_dataset(datasets):
    for dataset in datasets:
        dataset_dict = {"@context": CROISSANT_CONTEXT, "@id": dataset.id}
        for key, val in dataset.properties.items():
            if key == "type":
                dataset_dict["@type"] = val
            else:
                dataset_dict[key] = val
    return dataset_dict


def map_fileObjects(fileObject):
    fileObject_dict = {"@id": fileObject.id}
    for key, val in fileObject.properties.items():
        if key == "type":
            fileObject_dict["@type"] = val
        else:
            fileObject_dict[key] = val
    return fileObject_dict


def map_recordSet(recordSet):
    fields = []
    for field in recordSet.fields:
        fields.append(map_field(field))

    recordSet_dict = {"@id": recordSet.id}
    for key, val in recordSet.properties.items():
        if key == "type":
            recordSet_dict["@type"] = val
        else:
            recordSet_dict[key] = val
    recordSet_dict["field"] = fields
    return recordSet_dict


def map_field(field):
    statistics = []
    for statistic in field.statistics:
        statistics.append(map_statistics(statistic))

    field_dict = {"@id": field.id}
    for key, val in field.properties.items():
        if key == "type":
            field_dict["@type"] = val
        else:
            field_dict[key] = val

    field_dict["statistics"] = statistics[0] if statistics else None
    return field_dict


def map_statistics(statistic):
    statistic_dict = {"@id": statistic.id, "@type": "dg:ColumnStatistics"}
    for key, val in statistic.properties.items():
        if key == "type":
            statistic_dict["@type"] = val
        else:
            statistic_dict[key] = val
    return statistic_dict
