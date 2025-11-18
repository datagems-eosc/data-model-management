import os
import duckdb


DB_HOST = os.getenv("DATAGEMS_POSTGRES_HOST")
DB_PORT = os.getenv("DATAGEMS_POSTGRES_PORT")
DB_USER = os.getenv("DS_READER_USER")
DB_PASSWORD = os.getenv("DS_READER_PS")

DB_NAME = "ds_era5_land"

connection_string = (
    f"dbname={DB_NAME} "
    f"user={DB_USER} "
    f"password={DB_PASSWORD} "
    f"host={DB_HOST} "
    f"port={DB_PORT}"
)

try:
    con = duckdb.connect()
    con.sql("INSTALL postgres;")
    con.sql("LOAD postgres;")
    print("DuckDB PostgreSQL extension loaded successfully.")

    attach_query = f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);"
    con.sql(attach_query)
    print(f"Successfully attached PostgreSQL database '{DB_NAME}' as schema 'pg_db'.")

    remote_query = "SELECT COUNT(*) FROM pg_db.public.my_data_model_table;"

    result_df = con.sql(remote_query).fetchdf()

    print("\n--- Query Results ---")
    print(result_df)

except Exception as e:
    print(f"\nAn error occurred during database connection or querying: {e}")

finally:
    if "con" in locals():
        con.close()
        print("\nDuckDB connection closed.")
