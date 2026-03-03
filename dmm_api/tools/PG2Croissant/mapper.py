from dmm_api.tools.PG2Croissant.model import Dataset, FileObject, RecordSet, Field, ColumnStatistics

CONTEXT = {
    "@language": "en",
    "@vocab": "https://schema.org/",
    "access": "dg:access",
    "citeAs": "cr:citeAs",
    "column": "cr:column",
    "conformsTo": "dct:conformsTo",
    "cr": "http://mlcommons.org/croissant/",
    "data": {
      "@id": "cr:data",
      "@type": "@json"
    },
    "dataType": {
      "@id": "cr:dataType",
      "@type": "@vocab"
    },
    "dct": "http://purl.org/dc/terms/",
    "dg": "http://datagems.eu/TBD/",
    "doi": "dg:doi",
    "examples": {
      "@id": "cr:examples",
      "@type": "@json"
    },
    "extract": "cr:extract",
    "field": "cr:field",
    "fieldOfScience": "dg:fieldOfScience",
    "fileObject": "cr:fileObject",
    "fileProperty": "cr:fileProperty",
    "fileSet": "cr:fileSet",
    "format": "cr:format",
    "histogram": "dg:histogram",
    "includes": "cr:includes",
    "isLiveDataset": "cr:isLiveDataset",
    "jsonPath": "cr:jsonPath",
    "key": "cr:key",
    "max": "dg:max",
    "md5": "cr:md5",
    "mean": "dg:mean",
    "median": "dg:median",
    "min": "dg:min",
    "missingCount": "dg:missingCount",
    "missingPercentage": "dg:missingPercentage",
    "parentField": "cr:parentField",
    "path": "cr:path",
    "rai": "http://mlcommons.org/croissant/RAI/",
    "recordSet": "cr:recordSet",
    "references": "cr:references",
    "regex": "cr:regex",
    "repeated": "cr:repeated",
    "replace": "cr:replace",
    "rowCount": "dg:rowCount",
    "sc": "https://schema.org/",
    "separator": "cr:separator",
    "source": "cr:source",
    "standardDeviation": "dg:standardDeviation",
    "statistics": "dg:statistics",
    "status": "dg:status",
    "subField": "cr:subField",
    "transform": "cr:transform",
    "uniqueCount": "dg:uniqueCount",
    "uploadedBy": "dg:uploadedBy",
    "wd": "https://www.wikidata.org/wiki/"
  }

def map_to_croissant_heavyProfile(datasets):
    for dataset in datasets:
        distribution = []
        recordSets = []
        for fileObject in dataset.distribution:
            distribution.append(map_fileObjects(fileObject))
        for recordSet in dataset.recordSets:
            recordSets.append(map_recordSet(recordSet))

        dataset_dict = {    
            "@context": CONTEXT,
            "@type": "Dataset",
            "@id": dataset.id,
            "distribution": distribution,
            "recordSets": recordSets}
        for key,val in dataset.properties.items():
            dataset_dict[key] = val
    return dataset_dict   

def map_to_croissant_lightProfile(datasets):
    for dataset in datasets:
        distribution = []
        for fileObject in dataset.distribution:
            distribution.append(map_fileObjects(fileObject))

        dataset_dict = {    
            "@context": CONTEXT,
            "@type": "Dataset",
            "@id": dataset.id,
            "distribution": distribution}
        for key,val in dataset.properties.items():
            dataset_dict[key] = val
    return dataset_dict

def map_to_croissant_dataset(datasets):
    for dataset in datasets:    
        dataset_dict = {    
            "@context": CONTEXT,    
            "@type": "Dataset",
            "@id": dataset.id}
        for key,val in dataset.properties.items():
            dataset_dict[key] = val
    return dataset_dict

def map_fileObjects(fileObject):
    fileObject_dict = {"@id": fileObject.id}
    for key,val in fileObject.properties.items():
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
    for key,val in recordSet.properties.items():
        if key == "type":
            recordSet_dict["@type"] = val   
        else:   
            recordSet_dict[key] = val
    recordSet_dict["fields"] = fields
    return recordSet_dict


def map_field(field):
    statistics = []
    for statistic in field.statistics:
        statistics.append(map_statistics(statistic))
    
    field_dict = {"@id": field.id}
    for key,val in field.properties.items():
        if key == "type":
            field_dict["@type"] = val   
        else:
            field_dict[key] = val 

    field_dict["statistics"] = statistics  
    return field_dict

def map_statistics(statistic):
    statistic_dict = {"@id": statistic.id, 
                      "@type": "dg:ColumnStatistics"}
    for key,val in statistic.properties.items():
        if key == "type":
            statistic_dict["@type"] = val  
        else:
            statistic_dict[key] = val 
    return statistic_dict