import json 
from dmm_api.tools.PG2Croissant.parser import parse_pgjson
from dmm_api.tools.PG2Croissant.serializer import to_jsonld
from dmm_api.tools.PG2Croissant.mapper import map_to_croissant_dataset

def convert(pgjson_path:str, output_path:str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_pgjson(pgjson)
    croissant_dict = map_to_croissant_dataset(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python -m dmm_api.tools.PG2Croissant.main <input_file.json> <output_file.jsonld>")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    convert(input_file, output_file)
