PG_TO_CROISSANT_TYPES: {
    "integer": "integer", # type: ignore
    "bigint": "integer" # type: ignore
} # type: ignore

def map_table_to_recordset(table):
    fields = []
    for column in table.columns:
        croissant_type = PG_TO_CROISSANT_TYPES.get(column.type, "string")
        fields.append({ "name": column.name, "type": croissant_type})
    return {    "name": table.name, "fields": fields}

def map_to_croissant_dataset(tables):
    recordsets = []
    for table in tables:
        recordsets.append(map_table_to_recordset(table))
    return { "recordsets": recordsets}