import asyncio
import os
import sys
import warnings
import logging
from dotenv import load_dotenv
from pathlib import Path

# Suppress warnings from the ADK library
warnings.filterwarnings("ignore")
logging.getLogger('google.adk').setLevel(logging.CRITICAL)

# Explicitly load the .env file from the project's root directory.
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Correct imports based on the ADK tutorial
from sub_agents.bigquery.agent import build_bigquery_agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types

async def main():
    # --- 1. Get the User's Question ---
    # The question is read from the 'QUESTION' environment variable.
    question = os.getenv("QUESTION")
    if not question:
        print("Error: The QUESTION environment variable is not set.")
        return

    print(f">>> User Query: {question}")

    # --- 2. Build the Agent ---
    # This constructs the agent with its specific instructions and tools.
    agent = build_bigquery_agent()

    # --- 3. Set up Session and Runner ---
    # The ADK framework requires a session to manage the conversation history
    # and state, even for a single-turn interaction. The Runner is the engine
    # that orchestrates the agent's execution within this session.
    session_service = InMemorySessionService()
    runner = Runner(agent=agent, app_name="energy_agent_app", session_service=session_service)

    # Define a user and session ID for this specific run.
    user_id = "user_123"
    session_id_str = "session_abc_123"

    # Create the session in the session service.
    await session_service.create_session(
        app_name=runner.app_name, user_id=user_id, session_id=session_id_str
    )

    # Prepare the user's message in the format required by the ADK.
    user_message = genai_types.Content(role="user", parts=[genai_types.Part(text=question)])

    # --- 4. Run the Agent ---
    # The agent's reasoning process is executed here. Output is suppressed
    # to keep the final result clean.
    final_response = "Agent did not produce a final response."
    
    # Redirect stdout and stderr to suppress all agent output
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')
    
    try:
        async for event in runner.run_async(
            user_id=user_id, session_id=session_id_str, new_message=user_message
        ):
            if event.is_final_response() and event.content and event.content.parts:
                if event.content.parts[0].text:
                    final_response = event.content.parts[0].text.strip()
    finally:
        # Restore stdout and stderr
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = original_stdout
        sys.stderr = original_stderr

    # --- 5. Print the Final Result ---
    print("\n--- Final Answer ---")
    print(f"<<< Agent Response: {final_response}")

if __name__ == "__main__":
    asyncio.run(main())
