# from datetime import datetime
import os
import re
import duckdb


# We will implement different query execution methods based on the data type
def execute_query_csv(query, software):
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    try:
        # Query with DuckDB
        if software == "duckdb":
            s3_paths = re.findall(r"s3://dataset/[^\s,;]+", query)

            for s3_path in s3_paths:
                local_folder = s3_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}/*.csv')"
                s3_query = query.replace(s3_path, replacement)

            con = duckdb.connect(database=":memory:")
            result_df = con.execute(s3_query).fetchdf()
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501
