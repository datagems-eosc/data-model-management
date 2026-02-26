import json
from git import List
from model import Column, Table

def parse_pgjson(pgjson: dict) -> List[Table]:
    tables = []
    for table_name, table_info in pgjson.get("tables", {}).items():
        columns = []
        for column_name, column_info in table_info.get("columns", {}).items():
            column_type = column_info.get("type", "unknown")
            columns.append(Column(name=column_name, type=column_type))
        tables.append(Table(name=table_name, columns=columns))

    return tables
