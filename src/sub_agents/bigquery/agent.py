import os
from google.adk.agents import Agent
from google.genai import types

# Import the new and updated tools
from . import tools

# Read the method from environment variable, defaulting to BASELINE
NL2SQL_METHOD = os.getenv("NL2SQL_METHOD", "BASELINE")

def build_bigquery_agent():
    """Builds the BigQuery agent with its tools and setup callbacks."""
    
    # The initial_sql_tool is now the same for both methods
    initial_sql_tool = tools.initial_bq_nl2sql

    # New instructions for the agent
    base_instructions = """You are a BigQuery expert. Your goal is to answer user questions by writing and executing SQL queries.

Here is your workflow:
1.  Use the `list_bq_datasets` tool to find available datasets.
2.  Use the `get_schema_for_datasets` tool to get the schema for relevant dataset(s).
3.  Use the `initial_bq_nl2sql` tool to generate a SQL query.
4.  Use the `validate_and_execute_query` tool to run the SQL and get the answer.
"""

    # Get guidance from environment variable
    guidance = os.getenv("AGENT_GUIDANCE")
    if guidance:
        instructions = f"{base_instructions}\n\nFollow this additional guidance:\n{guidance}"
    else:
        instructions = base_instructions

    return Agent(
        model=os.getenv("BIGQUERY_AGENT_MODEL"),
        name="database_agent",
        instruction=instructions,
        tools=[
            initial_sql_tool,
            tools.validate_and_execute_query,
            tools.list_bq_datasets,
            tools.get_schema_for_datasets,
        ],
        # No longer need the before_agent_callback
        generate_content_config=types.GenerateContentConfig(temperature=0.0),
    )
