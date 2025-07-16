import json
import os
import uuid
import numpy as np
import pandas as pd


def create_json(csv_path, query):
    """
    Convert a CSV file to JSON format compatible with the API (requires 'nodes' field).
    """
    try:
        df = pd.read_csv(csv_path)
        df = df.replace({np.nan: None})
        data_records = df.to_dict(orient="records")

        new_dataset_uuid = str(uuid.uuid4())
        new_csv_uuid = str(uuid.uuid4())

        # "data": data_records is a fake RecordSet
        dataset_node = {
            "@context": {
                "@language": "en",
                "@vocab": "https://schema.org/",
                "citeAs": "cr:citeAs",
                "column": "cr:column",
                "conformsTo": "dct:conformsTo",
                "cr": "http://mlcommons.org/croissant/",
                "data": {"@id": "cr:data", "@type": "@json"},
                "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
                "examples": {"@id": "cr:examples", "@type": "@json"},
                "extract": "cr:extract",
                "field": "cr:field",
                "fileObject": "cr:fileObject",
                "fileProperty": "cr:fileProperty",
                "fileSet": "cr:fileSet",
                "format": "cr:format",
                "includes": "cr:includes",
                "isLiveDataset": "cr:isLiveDataset",
                "jsonPath": "cr:jsonPath",
                "key": "cr:key",
                "md5": "cr:md5",
                "parentField": "cr:parentField",
                "path": "cr:path",
                "rai": "http://mlcommons.org/croissant/RAI/",
                "recordSet": "cr:recordSet",
                "references": "cr:references",
                "regex": "cr:regex",
                "repeated": "cr:repeated",
                "replace": "cr:replace",
                "sc": "https://schema.org/",
                "separator": "cr:separator",
                "source": "cr:source",
                "subField": "cr:subField",
                "transform": "cr:transform",
                "wd": "https://www.wikidata.org/wiki/",
            },
            "@id": new_dataset_uuid,
            "@type": "sc:Dataset",
            "citeAs": "",
            "conformsTo": "",
            "country": "PT",
            "datePublished": "24-05-2025",
            "description": f"Dataset generated from the query: {query}",
            "distribution": [
                {
                    "@id": new_csv_uuid,
                    "@type": "cr:FileObject",
                    "contentSize": "2407043 B",
                    "contentUrl": csv_path,
                    "description": f"CSV generated from the query: {query}",
                    "encodingFormat": "text/csv",
                    "name": "csv_1.csv",
                    "sha256": "",
                }
            ],
            "data": data_records,
            "fieldOfScience": ["CIVIL ENGINEERING"],
            "headline": "Subway data.",
            "inLanguage": ["el"],
            "keywords": ["dev", "keyword"],
            "license": "???",
            "name": "Query_result",
            "url": "",
            "version": "",
        }

        json_path = os.path.join(os.path.dirname(csv_path), "results.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(dataset_node, f, indent=4, ensure_ascii=False)

        return dataset_node

    except Exception as e:
        raise Exception(f"Failed to create JSON dataset: {str(e)}")
