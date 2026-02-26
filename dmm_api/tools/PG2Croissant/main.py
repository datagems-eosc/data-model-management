import json 
from parser import parse_pgjson
from serializer import to_jsonld
from mapper import map_to_croissant_dataset

def convert(pgjson_path:str, output_path:str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    tables = parse_pgjson(pgjson)
    croissant_dict = map_to_croissant_dataset(tables)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)


if __name__ == "__main__":
    convert("pg_schema.json", "croissant_dataset.jsonld")