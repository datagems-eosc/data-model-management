import duckdb
import pandas as pd


def execute_query(dataset_name, query, software, csv_path):
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

            return {
                "query": replaced_query,
                # Convert to a CSV format
                "result": pd.DataFrame(result).to_csv(index=False),
            }

        else:
            return {"message": f"Unsupported software: {software}"}, 400

    except FileNotFoundError:
        return {"message": f"CSV file not found at path: {csv_path}"}
    except Exception as e:
        return {"message": f"Query execution failed: {str(e)}"}
