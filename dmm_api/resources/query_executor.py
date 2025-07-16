from datetime import datetime
import os
import duckdb


# We will implement different query execution methods based on the data type
def execute_query_csv(csv_name, query, software, data_path, user_id):
    software = software.lower()
    try:
        # Query with DuckDB
        if software == "duckdb":
            table_name = csv_name.replace(".csv", "")

            conn = duckdb.connect()
            # TODO: change to a PreparedStatements
            replaced_query = query.replace(
                f"FROM {table_name}", f"FROM read_csv_auto('{data_path}')"
            )
            result_df = conn.execute(replaced_query).fetchdf()
            conn.close()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_path = os.path.join(
                "dmm_api/data/results", f"{user_id}_{timestamp}"
            )
            os.makedirs(results_path, exist_ok=True)
            output_path = os.path.join(results_path, csv_name)
            result_df.to_csv(output_path, index=False, header=True)

            return output_path, query

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501
