"""
Module to initialize and provide shared services like LLM, Database, and Toolkits.
"""

from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_openai import OpenAI

from sensory.utils.databricks import get_sqlalchemy_engine
from sensory.react_agent.configuration import Configuration
from sensory.react_agent.utils import load_chat_model

# Load configuration
configuration = Configuration.from_context()

# Initialize LLM
llm = load_chat_model(configuration.model)

# Initialize Database
engine = get_sqlalchemy_engine()
db = SQLDatabase(
    engine=engine,
    lazy_table_reflection=True,
    include_tables=configuration.include_tables,
)

# Initialize SQLDatabaseToolkit
sql_toolkit = SQLDatabaseToolkit(db=db, llm=llm)


def get_llm():
    return llm


def get_db():
    return db


def get_sql_toolkit():
    return sql_toolkit
