# This test was created by the Langgraph CLI and depends on langsmith.
# Need to research this: https://docs.smith.langchain.com/evaluation

# import pytest

# from langsmith import unit
# from sensory.react_agent import graph


# @pytest.mark.skip
# @pytest.mark.asyncio
# @unit
# async def test_react_agent_simple_passthrough() -> None:
#     res = await graph.ainvoke(
#         {"messages": [("user", "Who is the founder of LangChain?")]},
#         {"configurable": {"system_prompt": "You are a helpful AI assistant."}},
#     )

#     assert "harrison" in str(res["messages"][-1].content).lower()
