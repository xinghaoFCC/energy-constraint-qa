from google.adk.tools import ToolContext
import google.generativeai as genai
import os
from tools.bigquery_io import execute_query
from tools.answers import format_results
from tools.schema import get_bigquery_schema, list_bigquery_datasets
from tools.validator import enforce

# Configure the client with the API key from environment variables
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise ValueError("The GOOGLE_API_KEY environment variable is not set.")
genai.configure(api_key=api_key)

# Get model name from environment variable and validate it
model_name = os.getenv("BASELINE_NL2SQL_MODEL")
if not model_name:
    raise ValueError("The BASELINE_NL2SQL_MODEL environment variable is not set. Please add it to your .env file.")

# Initialize the generative model to be used by the tools
llm_model = genai.GenerativeModel(model_name)

def get_schema_for_datasets(dataset_ids: str) -> str:
    """Retrieves the DDL schema for a comma-separated list of BigQuery dataset IDs."""
    dataset_id_list = [dataset.strip() for dataset in dataset_ids.split(',')]
    data_project_id = os.getenv("BQ_DATA_PROJECT_ID")
    compute_project_id = os.getenv("BQ_COMPUTE_PROJECT_ID")
    table_allowlist_str = os.getenv("BQ_TABLE_ALLOWLIST")
    table_allowlist = [table.strip() for table in table_allowlist_str.split(',')] if table_allowlist_str else None

    return get_bigquery_schema(
        dataset_ids=dataset_id_list,
        data_project_id=data_project_id,
        compute_project_id=compute_project_id,
        table_allowlist=table_allowlist
    )

def initial_bq_nl2sql(question: str, schema: str) -> str:
    """Generates an initial SQL query from a natural language question and a given schema."""

    prompt_template = """You are a BigQuery SQL expert.
Given a question and a database schema, your task is to generate a SQL query that answers the question.

###
DATABASE SCHEMA:
{SCHEMA}
###

QUESTION:
{QUESTION}

###
INSTRUCTIONS:
- The SQL query should be written for BigQuery.
- The query should be correct and executable.
- The query should be as simple as possible.
- The query should return a maximum of {MAX_NUM_ROWS} rows.
- Do not add any comments to the query.
- Do not add any text before or after the query.
- The query should be written in a single line.
"""

    MAX_NUM_ROWS = os.getenv('BQ_DEFAULT_LIMIT', '200')

    prompt = prompt_template.format(
        MAX_NUM_ROWS=MAX_NUM_ROWS, SCHEMA=schema, QUESTION=question
    )

    response = llm_model.generate_content(
        contents=prompt,
    )

    sql = response.text
    if sql:
        sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql

def validate_and_execute_query(sql_string: str) -> str:
    """Validates and then executes the given BigQuery SQL query."""
    try:
        enforce(sql_string, os.getenv('BQ_COMPUTE_PROJECT_ID'))
    except Exception as e:
        return f"Invalid SQL: {e}"
    
    try:
        results = execute_query(sql_string)
        return format_results(results)
    except Exception as e:
        return f"Error executing query: {e}"

def list_bq_datasets() -> list[str]:
    """Lists available BigQuery datasets.
    If BQ_DATASET_ID is set, it returns only that dataset.
    Otherwise, it lists all datasets in the configured BigQuery project.
    """
    # If a specific dataset is provided via BQ_DATASET_ID, only return that one.
    specific_dataset_id = os.getenv("BQ_DATASET_ID")
    if specific_dataset_id:
        return [specific_dataset_id]

    # Otherwise, list all datasets in the project.
    project_id = os.getenv("BQ_DATA_PROJECT_ID")
    if not project_id:
        return ["Error: BQ_DATA_PROJECT_ID environment variable is not set."]
    return list_bigquery_datasets(project_id)
