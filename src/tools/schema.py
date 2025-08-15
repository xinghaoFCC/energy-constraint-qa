from google.cloud import bigquery
import os

def get_bigquery_schema(dataset_ids, data_project_id, client=None, compute_project_id=None, table_allowlist=None):
    """Retrieves schema and generates DDL with example values for a list of BigQuery datasets."""

    if client is None:
        client = bigquery.Client(project=compute_project_id)

    all_ddl_statements = ""
    if not isinstance(dataset_ids, list):
        dataset_ids = [dataset_ids]

    for dataset_id in dataset_ids:
        dataset_ref = bigquery.DatasetReference(data_project_id, dataset_id)
        info_schema_query = f"""
            SELECT table_name
            FROM `{data_project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        """
        try:
            query_job = client.query(info_schema_query)
            for table_row in query_job.result():
                table_name = table_row.table_name
                if table_allowlist and table_name not in table_allowlist:
                    continue

                table_ref = dataset_ref.table(table_name)
                table_obj = client.get_table(table_ref)

                if table_obj.table_type == "TABLE":
                    column_defs = []
                    for field in table_obj.schema:
                        col_type = field.field_type
                        if field.mode == "REPEATED":
                            col_type = f"ARRAY<{col_type}>"
                        col_def = f"  `{field.name}` {col_type}"
                        if field.description:
                            escaped_description = field.description.replace("'", "''")
                            col_def += f" OPTIONS(description='''{escaped_description}''')"
                        column_defs.append(col_def)

                    ddl_statement = (
                        f"CREATE OR REPLACE TABLE `{table_ref}` "
                        f"(\n{',\n'.join(column_defs)}\n);\n\n"
                    )

                    try:
                        sample_query = f"SELECT * FROM `{table_ref}` LIMIT 2"
                        rows = client.query(sample_query).to_dataframe()

                        if not rows.empty:
                            ddl_statement += f"-- Example values for table `{table_ref}`:\n"
                            for _, row in rows.iterrows():
                                values_str = ", ".join(
                                    _serialize_value_for_sql(v) for v in row.values
                                )
                                ddl_statement += (
                                    f"INSERT INTO `{table_ref}` VALUES ({values_str});\n\n"
                                )
                    except Exception as e:
                        ddl_statement += f"-- NOTE: Could not retrieve sample rows for table {table_ref.path}.\n\n"

                    all_ddl_statements += ddl_statement
        except Exception as e:
            print(f"Could not query schema for dataset {dataset_id}: {e}")
            continue
    return all_ddl_statements


def list_bigquery_datasets(project_id, client=None):
    """Lists all datasets in a BigQuery project."""
    if client is None:
        client = bigquery.Client(project=project_id)
    
    datasets = list(client.list_datasets())
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    return dataset_ids


def _serialize_value_for_sql(value):
    """Serializes a Python value from a pandas DataFrame into a BigQuery SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return f"'{value.replace("'", "''")}'"
    return str(value)
