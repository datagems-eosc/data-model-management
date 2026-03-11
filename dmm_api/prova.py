from dmm_api.tools.AP.parse_AP import APRequest, extract_from_AP

ap_payload = {
    "edges": [
        {
            "from": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
            "labels": ["consistOf"],
            "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
        },
        {
            "from": "c893daaf-680f-4947-88e5-03fd61900795",
            "labels": ["input"],
            "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
        },
        {
            "from": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
            "labels": ["isAchieved"],
            "to": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
        },
        {
            "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "labels": ["request"],
            "to": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
        },
        {
            "from": "c893daaf-680f-4947-88e5-03fd61900795",
            "labels": ["distribution"],
            "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
        },
        {
            "from": "c893daaf-680f-4947-88e5-03fd61900795",
            "labels": ["distribution"],
            "to": "f5234567-890a-bcde-f012-3456789abcde",
        },
        {
            "from": "c893daaf-680f-4947-88e5-03fd61900795",
            "labels": ["recordSet"],
            "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
        },
        {
            "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
            "labels": ["field"],
            "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
        },
        {
            "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
            "labels": ["source/fileObject"],
            "to": "287a4312-6edc-4fbf-98ee-163c97704121",
        },
        {
            "from": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
            "labels": ["statistics"],
            "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
        },
    ],
    "nodes": [
        {
            "id": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
            "labels": ["analyticalPattern"],
            "properties": {
                "Description": "Analytical Pattern to update a dataset",
                "Name": "Update Dataset AP",
                "Process": "update",
                "PublishedDate": "2025-12-09",
                "StartTime": "10:00:00",
            },
        },
        {
            "id": "24a62ae9-41a9-472d-9a8a-438f35937980",
            "labels": ["DataModelManagement_Operator"],
            "properties": {
                "Description": "An operator to update a dataset into DataGEMS",
                "Name": "Update Operator",
                "PublishedDate": "2025-12-09",
                "Software": {},
                "StartTime": "10:00:00",
                "Step": 1,
                "command": "update",
            },
        },
        {"id": "c893daaf-680f-4947-88e5-03fd61900795", "labels": ["sc:Dataset"]},
        {
            "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
            "labels": ["CSV", "cr:FileObject"],
        },
        {
            "id": "f5234567-890a-bcde-f012-3456789abcde",
            "labels": ["CSV", "cr:FileObject"],
        },
        {
            "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
            "labels": ["cr:RecordSet"],
            "properties": {"name": "zoo-2024", "type": "cr:RecordSet"},
        },
        {
            "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
            "labels": ["cr:Field"],
            "properties": {
                "dataType": "sc:Text",
                "description": "",
                "name": "\u00ef\u00bb\u00bfKategorie",
                "sample": [
                    "S\u00c3\u00a4ugetiere",
                    "S\u00c3\u00a4ugetiere",
                    "S\u00c3\u00a4ugetiere",
                    "S\u00c3\u00a4ugetiere",
                    "V\u00c3\u00b6gel",
                    "V\u00c3\u00b6gel",
                    "Reptilien",
                    "S\u00c3\u00a4ugetiere",
                    "V\u00c3\u00b6gel",
                    "S\u00c3\u00a4ugetiere",
                ],
                "type": "cr:Field",
            },
        },
        {
            "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
            "labels": ["dg:ColumnStatistics"],
            "properties": {
                "histogram": "null",
                "max": "null",
                "mean": "null",
                "median": "null",
                "min": "null",
                "missingCount": 0,
                "missingPercentage": 0.0,
                "row_count": 192,
                "standardDeviation": "null",
                "type": "dg:ColumnStatistics",
                "uniqueCount": 190,
            },
        },
        {"id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96", "labels": ["User"]},
        {
            "id": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
            "labels": ["Task"],
            "properties": {
                "Description": "Task to update a dataset",
                "Name": "Dataset Updating Task",
            },
        },
    ],
}

ap_request = APRequest(**ap_payload)


# result = extract_from_AP(ap_request, target_labels={"sc:Dataset", "cr:FileObject", "cr:RecordSet", "cr:Field", "dg:ColumnStatistics"})
result = extract_from_AP(ap_request)
print(result)
# ([{'id': '6bc80891-81bf-4890-9e47-44f6ee72a6c1', 'labels': ['dg:ColumnStatistics'], 'properties': {'histogram': 'null',
# 'max': 'null', 'mean': 'null', 'median': 'null', 'min': 'null', 'missingCount': 0, 'missingPercentage': 0.0,
# 'row_count': 192, 'standardDeviation': 'null', 'type': 'dg:ColumnStatistics', 'uniqueCount': 190}},
# {'id': '883b5c9b-408a-4dd8-8619-e34e664b9920', 'labels': ['CSV', 'cr:FileObject'], 'properties': {}},
# {'id': 'eb87b0f3-fb8a-4a24-8234-3da28b7398a0', 'labels': ['cr:RecordSet'], 'properties': {'name': 'zoo-2024',
# 'type': 'cr:RecordSet'}}, {'id': 'c893daaf-680f-4947-88e5-03fd61900795', 'labels': ['sc:Dataset'], 'properties': {}},
# {'id': 'c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1', 'labels': ['cr:Field'], 'properties': {'dataType': 'sc:Text',
# 'description': '', 'name': 'ï»¿Kategorie', 'sample': ['SÃ¤ugetiere', 'SÃ¤ugetiere', 'SÃ¤ugetiere', 'SÃ¤ugetiere',
# 'VÃ¶gel', 'VÃ¶gel', 'Reptilien', 'SÃ¤ugetiere', 'VÃ¶gel', 'SÃ¤ugetiere'], 'type': 'cr:Field'}},
# {'id': 'f5234567-890a-bcde-f012-3456789abcde', 'labels': ['CSV', 'cr:FileObject'], 'properties': {}}],
# [{'from': 'c893daaf-680f-4947-88e5-03fd61900795', 'to': '883b5c9b-408a-4dd8-8619-e34e664b9920', 'labels': ['distribution']},
# {'from': 'c893daaf-680f-4947-88e5-03fd61900795', 'to': 'f5234567-890a-bcde-f012-3456789abcde', 'labels': ['distribution']},
# {'from': 'c893daaf-680f-4947-88e5-03fd61900795', 'to': 'eb87b0f3-fb8a-4a24-8234-3da28b7398a0', 'labels': ['recordSet']},
# {'from': 'eb87b0f3-fb8a-4a24-8234-3da28b7398a0', 'to': 'c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1', 'labels': ['field']},
# {'from': '6bc80891-81bf-4890-9e47-44f6ee72a6c1', 'to': 'c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1', 'labels': ['statistics']}])


# curl -X GET -H "Content-Type: application/json" "https://datagems-dev.scayle.es/dmm/api/v1/dataset/search?nodeIds=056ff7ea-ac5a-4496-abc5-ad254ddf58fa&nodeIds=c893daaf-680f-4947-88e5-03fd61900795" | python3 -m json.tool
{
    "code": 200,
    "message": "Datasets retrieved successfully",
    "datasets": [
        {
            "nodes": [
                {
                    "id": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                    "labels": ["sc:Dataset"],
                    "properties": {
                        "country": "PT",
                        "citeAs": "",
                        "keywords": ["dev", "keyword"],
                        "inLanguage": ["el"],
                        "description": "Subway data",
                        "type": "sc:Dataset",
                        "version": "",
                        "url": "",
                        "datePublished": "24-05-2025",
                        "sc:archivedAt": "s3://dataset/056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                        "license": "Public Domain",
                        "fieldOfScience": ["CIVIL ENGINEERING"],
                        "name": "OASA Data",
                        "conformsTo": "",
                        "id": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                        "headline": "Subway data.",
                        "status": "loaded",
                    },
                },
                {
                    "id": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                    "labels": ["CSV", "cr:FileObject"],
                    "properties": {
                        "contentUrl": "s3://dataset/056ff7ea-ac5a-4496-abc5-ad254ddf58fa/weather_data_fr.csv",
                        "sha256": "6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad",
                        "contentSize": "2407043 B",
                        "name": "weather_data_fr.csv",
                        "encodingFormat": "text/csv",
                        "description": "",
                        "id": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                        "type": "cr:FileObject",
                    },
                },
            ],
            "edges": [
                {
                    "from": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                    "to": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                    "labels": ["distribution"],
                    "properties": {},
                }
            ],
        },
        {
            "nodes": [
                {
                    "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": ["cr:Field"],
                    "properties": {
                        "dataType": "sc:Text",
                        "name": "\u00ef\u00bb\u00bfKategorie",
                        "description": "",
                        "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                        "type": "cr:Field",
                        "sample": [
                            "S\u00c3\u00a4ugetiere",
                            "S\u00c3\u00a4ugetiere",
                            "S\u00c3\u00a4ugetiere",
                            "S\u00c3\u00a4ugetiere",
                            "V\u00c3\u00b6gel",
                            "V\u00c3\u00b6gel",
                            "Reptilien",
                            "S\u00c3\u00a4ugetiere",
                            "V\u00c3\u00b6gel",
                            "S\u00c3\u00a4ugetiere",
                        ],
                    },
                },
                {
                    "id": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": ["CSV", "cr:FileObject"],
                    "properties": {
                        "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-metadata.csv",
                        "sha256": "7eg9d801g9d58644568c8c8c119g9g7eeg918585371ccd687g737d73208gb4be",
                        "contentSize": "1500000 B",
                        "name": "zoo-metadata.csv",
                        "description": "",
                        "encodingFormat": "text/csv",
                        "id": "f5234567-890a-bcde-f012-3456789abcde",
                        "type": "cr:FileObject",
                    },
                },
                {
                    "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                    "labels": ["Statistics"],
                    "properties": {
                        "missingPercentage": 0.0,
                        "missingCount": 0,
                        "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                        "type": "dg:Statistics",
                        "uniqueCount": 190,
                        "row_count": 192,
                    },
                },
                {
                    "id": "c893daaf-680f-4947-88e5-03fd61900795",
                    "labels": ["sc:Dataset"],
                    "properties": {
                        "country": "CH",
                        "citeAs": "",
                        "keywords": ["zoo", "animals", "species"],
                        "description": "Zoo animal species data",
                        "inLanguage": ["de"],
                        "type": "sc:Dataset",
                        "version": "",
                        "url": "",
                        "datePublished": "2024-12-09",
                        "sc:archivedAt": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795",
                        "license": "Public Domain",
                        "fieldOfScience": ["BIOLOGICAL SCIENCES"],
                        "name": "zoo_tierarten_2024",
                        "conformsTo": "",
                        "id": "c893daaf-680f-4947-88e5-03fd61900795",
                        "headline": "Zoo animal species data from Zurich Zoo.",
                        "status": "loaded",
                    },
                },
                {
                    "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                    "labels": ["CSV", "cr:FileObject"],
                    "properties": {
                        "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-2024.csv",
                        "sha256": "6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad",
                        "contentSize": "2407043 B",
                        "name": "zoo-2024.csv",
                        "description": "",
                        "encodingFormat": "text/csv",
                        "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                        "type": "cr:FileObject",
                    },
                },
                {
                    "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": ["cr:RecordSet"],
                    "properties": {
                        "name": "zoo-2024",
                        "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                        "type": "cr:RecordSet",
                    },
                },
            ],
            "edges": [
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                    "labels": ["distribution"],
                    "properties": {},
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": ["distribution"],
                    "properties": {},
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": ["recordSet"],
                    "properties": {},
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": ["recordSet"],
                    "properties": {},
                },
                {
                    "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": ["field"],
                    "properties": {},
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": ["recordSet"],
                    "properties": {},
                },
                {
                    "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": ["field"],
                    "properties": {},
                },
                {
                    "from": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                    "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": ["statistics"],
                    "properties": {},
                },
            ],
        },
    ],
}
