import re
from google.cloud import bigquery

def enforce(sql_string: str, compute_project_id: str):
    """Enforces that the SQL query is read-only and valid."""
    # More restrictive check for BigQuery - disallow DML and DDL
    if re.search(
        r"(?i)\b(update|delete|drop|insert|create|alter|truncate|merge)\b", sql_string
    ):
        raise ValueError("Invalid SQL: Contains disallowed DML/DDL operations.")

    # Use BigQuery's dry-run feature to validate the query without executing it
    client = bigquery.Client(project=compute_project_id)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    client.query(sql_string, job_config=job_config)  # This will raise an exception if the SQL is invalid
