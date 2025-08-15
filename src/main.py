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

# --- Control for showing reasoning process ---
SHOW_REASONING = os.getenv("SHOW_REASONING", "False").lower() in ("true", "1", "t")

async def main():
    # --- 1. Get the User's Question ---
    question = os.getenv("QUESTION")
    if not question:
        print("Error: The QUESTION environment variable is not set.")
        return

    print(f">>> User Query: {question}")

    # --- 2. Build the Agent ---
    agent = build_bigquery_agent()

    # --- 3. Set up Session and Runner ---
    session_service = InMemorySessionService()
    runner = Runner(agent=agent, app_name="energy_agent_app", session_service=session_service)

    user_id = "user_123"
    session_id_str = "session_abc_123"

    await session_service.create_session(
        app_name=runner.app_name, user_id=user_id, session_id=session_id_str
    )

    user_message = genai_types.Content(role="user", parts=[genai_types.Part(text=question)])

    # --- 4. Run the Agent ---
    final_response = "Agent did not produce a final response."

    if SHOW_REASONING:
        print("\n--- Agent Reasoning Process ---")
        async for event in runner.run_async(
            user_id=user_id, session_id=session_id_str, new_message=user_message
        ):
            author = event.author
            if event.get_function_calls():
                for fc in event.get_function_calls():
                    print(f"[{author}]: Tool Call: {fc.name}({fc.args})")
            if event.get_function_responses():
                for fr in event.get_function_responses():
                    print(f"[{author}]: Tool Response: {fr.name} -> {fr.response}")
            if event.content and event.content.parts and event.content.parts[0].text:
                print(f"[{author}]: {event.content.parts[0].text.strip()}")

            if event.is_final_response() and event.content and event.content.parts:
                if event.content.parts[0].text:
                    final_response = event.content.parts[0].text.strip()
    else:
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
            sys.stdout.close()
            sys.stderr.close()
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    # --- 5. Print the Final Result ---
    print("\n--- Final Answer ---")
    print(f"<<< Agent Response: {final_response}")

if __name__ == "__main__":
    asyncio.run(main())
