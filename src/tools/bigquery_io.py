from google.cloud import bigquery
import os

def execute_query(sql: str):
    """Executes a BigQuery query and returns the results."""
    client = bigquery.Client(project=os.getenv('BQ_COMPUTE_PROJECT_ID'))
    query_job = client.query(sql)
    results = query_job.result()
    return results
