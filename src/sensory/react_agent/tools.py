"""
This module provides example tools for web scraping and search
functionality.

It includes a basic Tavily search function (as an example)

These tools are intended as free examples to get started. For production use,
consider implementing more robust and specialized tools tailored to your needs.
"""

from typing import Any, Callable, List, Optional, cast

from langchain_tavily import TavilySearch

from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit

from sensory.react_agent.configuration import Configuration

from sensory.utils.databricks import get_sqlalchemy_engine
from sensory.react_agent.utils import load_chat_model

from databricks_langchain import DatabricksVectorSearch
from langchain_openai import OpenAIEmbeddings


async def search(query: str) -> Optional[dict[str, Any]]:
    """Search for general web results.

    This function performs a search using the Tavily search engine, which is
    designed to provide comprehensive, accurate, and trusted results.
    It's particularly useful for answering questions about current events.
    """
    configuration = Configuration.from_context()
    wrapped = TavilySearch(max_results=configuration.max_search_results)
    return cast(dict[str, Any], await wrapped.ainvoke({"query": query}))


async def vector_search_tool(
    query: str, k: int = 5, filter: dict | None = None
) -> list[str]:
    """Semantic search using Databricks Vector Search (HYBRID).

    Args:
        query (str): The search query.
        k (int, optional): Number of results to return. Defaults to 5.
        filter (dict, optional): Optional filter for metadata search. Defaults to None.

    Returns:
        list[str]: List of full documents (as strings) matching the query.
    """
    configuration = Configuration.from_context()
    embed_model = OpenAIEmbeddings(model="text-embedding-3-small")
    vector_store = DatabricksVectorSearch(
        endpoint=configuration.vectorsearch_endpoint,
        index_name=configuration.vectorsearch_index,
        embedding=embed_model,
        text_column="data",
        query_type="HYBRID",
    )
    results = await vector_store.asimilarity_search(
        query=query,
        k=k,
        filter=filter,
    )
    # Each result is a Document with .page_content containing the full document (str/json)
    return [doc.page_content for doc in results]


# Initialize the SQL Database Toolkit
engine = get_sqlalchemy_engine()
# Moved up to access include_tables for SQLDatabase initialization
configuration = Configuration.from_context()
db = SQLDatabase(
    engine=engine,
    lazy_table_reflection=True,
    include_tables=configuration.include_tables,  # Use configured tables
)
# TODO: The llm instance needs to be passed here.
# This might require a refactor of how tools are initialized,
# potentially moving this initialization into the graph.py or a factory
# function. For now, we'll load a default model as a placeholder.
# This will be addressed in a subsequent step.
llm = load_chat_model(configuration.model)
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
sql_tools = toolkit.get_tools()

TOOLS: List[Callable[..., Any]] = [vector_search_tool] + sql_tools
