from dmm_api.tools.PG2Croissant.model import Dataset, FileObject, RecordSet, Field, ColumnStatistics



def map_to_croissant_dataset(datasets):
    for dataset in datasets:
        distribution = []
        recordSets = []
        for fileObject in dataset.distribution:
            distribution.append(map_fileObjects(fileObject))
        for recordSet in dataset.recordSets:
            recordSets.append(map_recordSet(recordSet))

        dataset_dict = {    
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": dataset.id,
            "distribution": distribution,
            "recordSets": recordSets}
        for key,val in dataset.properties.items():
            dataset_dict[key] = val
    return dataset_dict   


def map_fileObjects(fileObject):
    fileObject_dict = {"@id": fileObject.id}
    for key,val in fileObject.properties.items():
        fileObject_dict[key] = val
    return fileObject_dict

def map_recordSet(recordSet):
    fields = []
    for field in recordSet.fields:
        fields.append(map_field(field))

    recordSet_dict = {"@id": recordSet.id}
    for key,val in recordSet.properties.items():
        recordSet_dict[key] = val
    recordSet_dict["fields"] = fields
    return recordSet_dict


def map_field(field):
    statistics = []
    for statistic in field.statistics:
        statistics.append(map_statistics(statistic))
    
    field_dict = {"@id": field.id}
    for key,val in field.properties.items():
        field_dict[key] = val 

    field_dict["statistics"] = statistics  
    return field_dict

def map_statistics(statistic):
    statistic_dict = {"@id": statistic.id}
    for key,val in statistic.properties.items():
        statistic_dict[key] = val 
    return statistic_dict