# TODO: Add DatabricksVectorSearch Tool Integration

## Goal
Integrate DatabricksVectorSearch as a tool for the agent, enabling semantic search over the `master_sensory_panel_joined_index` index.

## References
- [LangChain DatabricksVectorSearch Docs](https://python.langchain.com/docs/integrations/vectorstores/databricks_vector_search/)
- [Databricks AI Bridge LangChain Integration](https://github.com/databricks/databricks-ai-bridge/tree/main/integrations/langchain/src/databricks_langchain)
- See `/notebooks/vector_index.ipynb` for endpoint and index details.

---

## Plan / TODO List

- [x] **Install Required Packages**
  - Ensure `databricks-vectorsearch`, `databricks-langchain`, and `langchain-openai` are installed (already in `%pip install` in notebook).

- [x] **Configuration**
  - Add configuration options for:
    - Databricks Vector Search endpoint name (e.g., `master_sensory_data_endpoint`)
    - Index name (e.g., `manufacturing_dev.work_agent_barney.master_sensory_panel_joined_index`)
  - Only support one endpoint/index for now.

- [ ] **Implement the Tool**
  - Create an async function (e.g., `vector_search_tool`) that:
    - Accepts: `query: str`, `k: int = 5`, `filter: Optional[dict] = None`
    - Initializes `DatabricksVectorSearch` with:
      - `endpoint` and `index_name` from configuration
      - `embedding` as `OpenAIEmbeddings(model="text-embedding-3-small")`
      - `text_column='data'`
      - `query_type="HYBRID"`
    - Calls `.similarity_search()` with support for filter.
    - Returns only the full document (which includes all metadata fields).

- [ ] **Add Tool to Agent**
  - Import and append the new tool to the `TOOLS` list in `src/sensory/react_agent/tools.py`.

- [ ] **Testing**
  - Add a test or notebook cell to verify the tool works as expected with a sample query and filter.

- [ ] **Documentation**
  - Document the new tool in the code and update any relevant README or usage docs.

---

## Notes

- The tool should always return the full document (including all metadata).
- The agent should be able to call the tool with filters if needed.
- Only one endpoint/index is supported for now for simplicity.

---
