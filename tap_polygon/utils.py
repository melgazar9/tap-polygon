import logging

def check_missing_fields(schema: dict, record: dict):
    schema_fields = set(schema.get("properties", {}).keys())
    record_keys = set(record.keys())

    missing_in_record = schema_fields - record_keys
    if missing_in_record:
        logging.warning(f"*** Missing fields in record that are present in schema: {missing_in_record} ***")

    missing_in_schema = record_keys - schema_fields
    if missing_in_schema:
        logging.critical(
            f"*** Missing fields in schema that are present record: {missing_in_schema} ***"
        )