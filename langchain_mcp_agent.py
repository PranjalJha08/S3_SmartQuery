from langchain.tools import Tool
from langchain.agents import initialize_agent, AgentType
from langchain_openai import OpenAI
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# Check for OpenAI API key
if not os.environ.get("OPENAI_API_KEY"):
    raise EnvironmentError("OPENAI_API_KEY not found. Please set it in your environment or .env file.")

# Define the MCP tool
def query_mcp_server(question):
    response = requests.post("http://localhost:5000/query", json={"query": question})
    return response.json()["result"]

mcp_tool = Tool(
    name="S3 SmartQuery MCP",
    func=query_mcp_server,
    description="Use this tool to answer questions about S3 files, analytics, and storage."
)

# Initialize the LLM (ensure your OpenAI API key is set in your environment)
llm = OpenAI(temperature=0)

# Create the agent with the MCP tool
agent = initialize_agent(
    [mcp_tool],
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Example: Ask a question
if __name__ == "__main__":
    question = input("Ask a question about your S3 data: ")
    result = agent.run(question)
    print("Agent answer:", result)
