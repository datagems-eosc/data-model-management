import json
import duckdb
import pandas as pd


# We will implement different query execution methods based on the data type
def execute_query_csv(dataset_name, query, software, csv_path):
    software = software.lower()
    try:
        # Query with DuckDB
        if software == "duckdb":
            conn = duckdb.connect()
            # TO DO: change to a PreparedStatements
            replaced_query = query.replace(
                f"FROM {dataset_name}", f"FROM read_csv_auto('{csv_path}')"
            )
            result = conn.execute(replaced_query).fetchall()
            conn.close()

            df = pd.DataFrame(result)

            output_path = csv_path.replace("data/oasa", "data/results/oasa")
            output_path = output_path.replace(".csv", "_query_results.csv")
            df.to_csv(output_path, index=False)

            metadata = {
                "dataset_name": dataset_name,
                "executed_query": replaced_query,
                "csv_results_path": output_path,
            }

            json_path = output_path.replace("_query_results.csv", "_metadata.json")
            with open(json_path, "w") as f:
                json.dump(metadata, f, indent=4)

            return {
                "query": replaced_query,
                "result": pd.DataFrame(result).to_csv(index=False),
                "message": f"Results saved to CSV at {output_path} (available in JSON metadata)",
                "json_metadata_path": json_path,
            }

        else:
            return {"message": f"Unsupported software: {software}"}, 400

    except Exception as e:
        return {"message": f"Query execution failed: {str(e)}"}


# To be implemented
def execute_query_xml(dataset_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501
