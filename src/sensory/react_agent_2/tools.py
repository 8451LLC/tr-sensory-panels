"""
This module provides example tools for web scraping and search
functionality.

It includes a basic Tavily search function (as an example)

These tools are intended as free examples to get started. For production use,
consider implementing more robust and specialized tools tailored to your needs.
"""

from typing import Any, Callable, List, Optional, Union, cast

from databricks.vector_search.client import VectorSearchClient
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_openai import OpenAIEmbeddings
from langchain_tavily import TavilySearch
from pydantic import BaseModel, Field

from sensory.utils.databricks import get_sqlalchemy_engine
from sensory.react_agent_2.configuration import Configuration
from sensory.react_agent_2.utils import load_chat_model


class Filter(BaseModel):
    """
    Represents a single filter condition for vector search queries.

    Use this model to specify a filter on any column, with or without an operator.
    Filters can match exact values, perform comparisons, combine columns, or use LIKE
    for token-based string matching. See field descriptions for supported operators
    and usage examples.
    """

    key: str = Field(
        ...,
        description=(
            "The filter key, which can be a column name or a column name with an operator. "
            "Supported operators: NOT, <, <=, >, >=, OR, LIKE. "
            "Examples: 'color', 'price <', 'id NOT', 'color1 OR color2', 'column LIKE'."
        ),
    )
    value: Any = Field(
        ...,
        description=(
            "The filter value. Can be a string, number, or list of values depending on the operator. "
            "Examples: 'red', 200, ['red', 'blue']"
        ),
    )


class FilterList(BaseModel):
    filters: List[Filter] = Field(
        ...,
        description="A list of filter objects to apply to the query. Each filter represents a single filter condition.",
    )


async def search(query: str) -> Optional[dict[str, Any]]:
    """Search for general web results.

    This function performs a search using the Tavily search engine, which is
    designed to provide comprehensive, accurate, and trusted results.
    It's particularly useful for answering questions about current events.
    """
    configuration = Configuration.from_context()
    wrapped = TavilySearch(max_results=configuration.max_search_results)
    return cast(dict[str, Any], await wrapped.ainvoke({"query": query}))


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


def vector_search_summary_index(
    query_text: str, num_results: int = 3, filters: Optional[FilterList] = None
) -> list:
    """
    Perform similarity search on the summary index using Databricks Vector Search SDK.

    The summary index contains high-level information about each sensory test as a whole.
    Use this tool to retrieve general or aggregate details about sensory panels,
    products, or test events.

    Args:
        query_text (str): The search query.
        num_results (int, optional): Number of results to return. Defaults to 3.
        filters (FilterList, optional): List of filter objects to restrict search results. Defaults to no filters.
    """
    configuration = Configuration.from_context()
    embed_model = OpenAIEmbeddings(model="text-embedding-3-small")
    client = VectorSearchClient()
    summary_index = client.get_index(
        endpoint_name=configuration.vectorsearch_endpoint,
        index_name=configuration.vectorsearch_summary_index,
    )
    query_embedding = embed_model.embed_query(query_text)
    filter_dict = (
        {f.key: f.value for f in filters.filters} if filters is not None else {}
    )
    return summary_index.similarity_search(
        columns=["data"],
        query_text=query_text,
        query_vector=query_embedding,
        num_results=num_results,
        query_type="HYBRID",
        filters=filter_dict,
    )


def vector_search_responses_index(
    query_text: str, num_results: int = 3, filters: Optional[FilterList] = None
) -> list:
    """
    Perform similarity search on the responses index using Databricks Vector Search SDK.

    The responses index contains individual responses from each test and panelist.
    Use this tool to retrieve specific feedback, ratings, or comments from panelists
    for each sensory test.

    Args:
        query_text (str): The search query.
        num_results (int, optional): Number of results to return. Defaults to 3.
        filters (FilterList, optional): List of filter objects to restrict search results. Defaults to no filters.
    """
    configuration = Configuration.from_context()
    embed_model = OpenAIEmbeddings(model="text-embedding-3-small")
    client = VectorSearchClient()
    responses_index = client.get_index(
        endpoint_name=configuration.vectorsearch_endpoint,
        index_name=configuration.vectorsearch_responses_index,
    )
    query_embedding = embed_model.embed_query(query_text)
    filter_dict = (
        {f.key: f.value for f in filters.filters} if filters is not None else {}
    )
    return responses_index.similarity_search(
        columns=["data"],
        query_text=query_text,
        query_vector=query_embedding,
        num_results=num_results,
        query_type="HYBRID",
        filters=filter_dict,
    )


TOOLS: List[Callable[..., Any]] = [
    vector_search_summary_index,
    vector_search_responses_index,
] + sql_tools
