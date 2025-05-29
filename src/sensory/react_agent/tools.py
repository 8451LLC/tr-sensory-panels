"""
This module provides example tools for web scraping and search
functionality.

It includes a basic Tavily search function (as an example)

These tools are intended as free examples to get started. For production use,
consider implementing more robust and specialized tools tailored to your needs.
"""

from typing import Any, Callable, List, Optional, cast

from databricks.vector_search.client import VectorSearchClient
from langchain_tavily import TavilySearch

from sensory.react_agent.configuration import Configuration
from sensory.react_agent.shared_services import (
    get_sql_toolkit,
)  # Import the shared toolkit
from langchain_openai import OpenAIEmbeddings  # Keep this for vector search


async def search(query: str) -> Optional[dict[str, Any]]:
    """Search for general web results.

    This function performs a search using the Tavily search engine, which is
    designed to provide comprehensive, accurate, and trusted results.
    It's particularly useful for answering questions about current events.
    """
    configuration = Configuration.from_context()
    wrapped = TavilySearch(max_results=configuration.max_search_results)
    return cast(dict[str, Any], await wrapped.ainvoke({"query": query}))


# Access the toolkit via the shared_services module
sql_toolkit = get_sql_toolkit()
sql_tools = sql_toolkit.get_tools()


def vector_search_summary_index(query_text: str, num_results: int = 3) -> list:
    """
    Perform similarity search on the summary index using Databricks Vector Search SDK.

    The summary index contains high-level information about each sensory test as a whole.
    Use this tool to retrieve general or aggregate details about sensory panels,
    products, or test events.
    """
    configuration = Configuration.from_context()
    embed_model = OpenAIEmbeddings(model="text-embedding-3-small")
    client = VectorSearchClient()
    summary_index = client.get_index(
        endpoint_name=configuration.vectorsearch_endpoint,
        index_name=configuration.vectorsearch_summary_index,
    )
    query_embedding = embed_model.embed_query(query_text)
    return summary_index.similarity_search(
        columns=["data"],
        query_text=query_text,
        query_vector=query_embedding,
        num_results=num_results,
        query_type="HYBRID",
        filters={},
    )


def vector_search_responses_index(query_text: str, num_results: int = 3) -> list:
    """
    Perform similarity search on the responses index using Databricks Vector Search SDK.

    The responses index contains individual responses from each test and panelist.
    Use this tool to retrieve specific feedback, ratings, or comments from panelists
    for each sensory test.
    """
    configuration = Configuration.from_context()
    embed_model = OpenAIEmbeddings(model="text-embedding-3-small")
    client = VectorSearchClient()
    responses_index = client.get_index(
        endpoint_name=configuration.vectorsearch_endpoint,
        index_name=configuration.vectorsearch_responses_index,
    )
    query_embedding = embed_model.embed_query(query_text)
    return responses_index.similarity_search(
        columns=["data"],
        query_text=query_text,
        query_vector=query_embedding,
        num_results=num_results,
        query_type="HYBRID",
        filters={},
    )


TOOLS: List[Callable[..., Any]] = [
    vector_search_summary_index,
    vector_search_responses_index,
] + sql_tools
