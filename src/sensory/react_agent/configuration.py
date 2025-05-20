"""Define the configurable parameters for the agent."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Annotated

from langchain_core.runnables import ensure_config
from langgraph.config import get_config

from sensory.react_agent import prompts


@dataclass(kw_only=True)
class Configuration:
    """The configuration for the agent."""

    system_prompt: str = field(
        default=prompts.SYSTEM_PROMPT,
        metadata={
            "description": (
                "The system prompt to use for the agent's interactions. "
                "This prompt sets the context and behavior for the agent."
            )
        },
    )

    model: Annotated[str, {"__template_metadata__": {"kind": "llm"}}] = field(
        # default="anthropic/claude-3-5-sonnet-20240620",
        default="openai/gpt-4o",
        metadata={
            "description": (
                "The name of the language model to use for the agent's main "
                "interactions. Should be in the form: provider/model-name."
            )
        },
    )

    max_search_results: int = field(
        default=10,
        metadata={
            "description": (
                "The maximum number of search results to return for each "
                "search query."
            )
        },
    )

    include_tables: list[str] = field(
        default_factory=lambda: [
            "master_sensory_panel_joined_silver",
            "master_sensory_responses_collected_silver",
        ],
        metadata={
            "description": (
                "A list of table names to include for the SQLDatabaseToolkit. "
                "Only these tables will be accessible to the agent."
            )
        },
    )

    # Databricks Vector Search configuration
    vectorsearch_endpoint: str = field(
        default="master_sensory_data_endpoint",
        metadata={
            "description": (
                "The Databricks Vector Search endpoint name to use for vector search queries."
            )
        },
    )
    vectorsearch_index: str = field(
        default="manufacturing_dev.work_agent_barney.master_sensory_panel_joined_index",
        metadata={
            "description": (
                "The Databricks Vector Search index name to use for vector search queries."
            )
        },
    )

    @classmethod
    def from_context(cls) -> Configuration:
        """Create a Configuration instance from a RunnableConfig object."""
        try:
            config = get_config()
        except RuntimeError:
            config = None
        config = ensure_config(config)
        configurable = config.get("configurable") or {}
        _fields = {f.name for f in fields(cls) if f.init}
        return cls(**{k: v for k, v in configurable.items() if k in _fields})
