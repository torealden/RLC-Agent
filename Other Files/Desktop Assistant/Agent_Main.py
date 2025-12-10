# agent_main.py
import os
from dotenv import load_dotenv

from langchain import hub
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.messages import SystemMessage
from langchain_community.chat_models import ChatOllama  # NEW
from langchain_google_community import GmailToolkit
from langchain_google_community import CalendarToolkit

gmail_toolkit = GmailToolkit()  # Uses your credentials
calendar_toolkit = CalendarToolkit()
tools = gmail_toolkit.get_tools() + calendar_toolkit.get_tools() + notion_tools  # From your notion_tools.py

llm = ChatOllama(model="llama3.1", base_url="http://localhost:11434")
agent = create_react_agent(llm, tools, prompt=hub.pull("hwchase17/react"))
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

from google_tools import (
    search_gmail,
    read_gmail_message,
    send_gmail,
    list_calendar_events,
    create_calendar_event,
)
from notion_tools import (
    notion_find_tasks,
    notion_add_task,
    notion_update_task_status,
)

def make_agent():
    load_dotenv()

    # --- LLM via Ollama ---
    model_name = os.getenv("OLLAMA_MODEL", "gemma3:4b")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    llm = ChatOllama(model=model_name, base_url=base_url, temperature=0.2)

    tools = [
        # Gmail/Calendar
        search_gmail,
        read_gmail_message,
        # comment out if you don’t want send yet:
        send_gmail,
        list_calendar_events,
        create_calendar_event,
        # Notion
        notion_find_tasks,
        notion_add_task,
        notion_update_task_status,
    ]

    system = SystemMessage(content=(
        "You are Round Lakes Assistant. You can use tools for Gmail, Google Calendar, and Notion. "
        "Prefer precise, stepwise plans; summarize clearly; and use tools when tasks require data. "
        "Parse natural dates to ISO with timezone when scheduling. If required info is missing, ask briefly."
    ))

    prompt = hub.pull("hwchase17/react")
    agent = create_react_agent(llm=llm, tools=tools, prompt=prompt)
    executor = AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True)
    return executor, system

def cli():
    executor, system = make_agent()
    print("✅ Agent w/ Ollama + Gmail/Calendar + Notion ready.")
    print("Examples:")
    print(" - 'Find unread emails about invoices from last 7 days and summarize.'")
    print(" - 'List my meetings next week.'")
    print(" - 'Add a Notion task: Draft HOBO deck, due next Friday 5pm.'")
    print(" - 'Mark the Notion task “Draft HOBO deck” as Done.'")
    while True:
        try:
            user_inp = input("\nYou> ").strip()
            if user_inp.lower() in {"quit", "exit"}:
                break
            result = executor.invoke({"input": user_inp, "messages": [system]})
            print("\nAssistant>", result["output"])
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    cli()
