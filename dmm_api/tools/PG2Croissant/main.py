import json 
from dmm_api.tools.PG2Croissant.parser import parse_lightProfile, parse_heavyProfile, parse_dataset
from dmm_api.tools.PG2Croissant.serializer import to_jsonld
from dmm_api.tools.PG2Croissant.mapper import map_to_croissant_dataset, map_to_croissant_lightProfile, map_to_croissant_heavyProfile

def convertDataset(pgjson_path:str, output_path:str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_dataset(pgjson)
    croissant_dict = map_to_croissant_dataset(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)

def convertLightProfile(pgjson_path:str, output_path:str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_lightProfile(pgjson)
    croissant_dict = map_to_croissant_lightProfile(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)

def convertHeavyProfile(pgjson_path:str, output_path:str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_heavyProfile(pgjson)
    croissant_dict = map_to_croissant_heavyProfile(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python -m dmm_api.tools.PG2Croissant.main <input_file.json> <output_file.jsonld> <type>")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    type = sys.argv[3] if len(sys.argv) > 3 else "dataset"
    if type == "dataset":
        convertDataset(input_file, output_file) 
    elif type == "lightProfile":
        convertLightProfile(input_file, output_file)
    elif type == "heavyProfile":
        convertHeavyProfile(input_file, output_file)

