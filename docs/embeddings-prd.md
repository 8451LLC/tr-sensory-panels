# Project Requirements: Embeddings for Sensory Panel Data

**Date:** May 19, 2025

**Project Goal:** To create vector embeddings for the `data` column in two key Databricks tables: `master_sensory_panel_joined_silver` and `master_sensory_responses_collected_silver`. These embeddings will enable semantic search and other machine learning applications on the sensory panel data.

## 1. Scope

This project will focus on the following:

*   **Data Sources:**
    *   `master_sensory_panel_joined_silver`: This table contains joined information about sensory panels. The `data` column is a struct containing various attributes of the panel.
    *   `master_sensory_responses_collected_silver`: This table contains individual panelist responses. The `data` column is a struct containing test details and an array of question-response pairs.
*   **Column to Embed:** The `data` column in both specified tables. The entire content of the `data` struct should be serialized (e.g., to JSON string) before embedding.
*   **Embedding Model:** OpenAI `text-embedding-3-small`.
*   **Embedding Process:**
    *   Retrieve data from the source tables using the `SQLWarehouse` utility.
    *   Implement batch embedding to process records efficiently. Determine a reasonable batch size based on API limits and performance considerations.
    *   Store the generated embeddings back into new columns (e.g., `data_embedding`) in the respective tables or new tables, as appropriate. The `SQLWarehouse` utility should be used for this insertion/update.
*   **Technology Stack:**
    *   Python
    *   LangChain or OpenAI SDK for interacting with the embedding model.
    *   Databricks SQL Connector (as used in `SQLWarehouse`).

## 2. Requirements

### 2.1. Data Retrieval and Preparation

*   Develop a script or function to connect to Databricks and retrieve records from `master_sensory_panel_joined_silver`.
*   Develop a script or function to connect to Databricks and retrieve records from `master_sensory_responses_collected_silver`.
*   For each table, extract the `data` column.
*   Serialize the content of the `data` column (which is a struct or contains complex types like arrays of structs) into a single string representation suitable for the embedding model (e.g., JSON string). Handle null or missing `data` columns gracefully.

### 2.2. Embedding Generation

*   Utilize the OpenAI `text-embedding-3-small` model via either the LangChain library or the official OpenAI Python SDK.
*   Implement batching for sending data to the embedding API to optimize for cost and rate limits. The batch size should be configurable.
*   Handle potential errors during the embedding process, such as API errors or network issues, with appropriate retry mechanisms or logging.

### 2.3. Data Storage

*   Define the schema for storing the embeddings. This will likely involve adding a new column of type `ARRAY<FLOAT>` or `VECTOR` (if supported and appropriate for Databricks Delta tables) to the existing tables or creating new tables that link back to the original records.
*   Use the `SQLWarehouse.query()` method (or similar functionality for DML statements) to insert or update the records with the newly generated embeddings.
*   Ensure the process is idempotent where possible, or includes logic to avoid re-processing already embedded records unless explicitly required.

### 2.4. Code and Utilities

*   Leverage the existing `SQLWarehouse` class in `src/sensory/utils/databricks.py` for all Databricks SQL interactions (retrieval and storage).
*   Structure the code logically, potentially within the `src/sensory/embeddings/` directory.
*   Include clear comments and documentation within the code.

## 3. Technical Specifications

*   **Embedding Model:** `text-embedding-3-small` (OpenAI)
*   **Databricks Interaction:** Use the provided `SQLWarehouse` class.
*   **Batch Size:** To be determined, but should be a reasonable number (e.g., 50-200 records per batch, depending on average text size and API constraints).
*   **Error Handling:** Implement robust error handling and logging for the embedding process.

## 4. Deliverables

*   Python script(s) capable of:
    *   Connecting to Databricks.
    *   Retrieving data from the specified tables.
    *   Serializing the `data` column.
    *   Generating embeddings using the specified model and batching.
    *   Storing the embeddings back into Databricks.
*   This Project Requirement Document (`docs/embeddings-prd.md`).
*   (Optional) Jupyter notebooks for experimentation and demonstration.

## 5. Assumptions

*   Necessary Databricks credentials (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`) are configured in the environment.
*   OpenAI API key (`OPENAI_API_KEY` and potentially `OPENAI_API_BASE`) is configured in the environment.
*   The `SQLWarehouse` utility is functional and can execute `SELECT` queries as well as DML statements (e.g., `ALTER TABLE ADD COLUMN`, `UPDATE`, `INSERT`). If `UPDATE` is complex for structs, alternative strategies like creating new tables might be needed.
*   The `data` column, when serialized, will not exceed the token limits of the `text-embedding-3-small` model for a single input.

## 6. Questions / Clarifications Needed

*   What is the preferred method for storing the embeddings?
    *   Add a new column to the existing `master_sensory_panel_joined_silver` and `master_sensory_responses_collected_silver` tables (e.g., `data_embedding VECTOR`)?
    *   Create new tables (e.g., `master_sensory_panel_embeddings_gold`, `master_sensory_responses_embeddings_gold`) that store the primary key(s) of the original table and the corresponding embedding?
*   How should updates be handled? If the source `data` changes, how will the corresponding embedding be updated? Is a full re-computation expected periodically, or an incremental update mechanism?
*   Are there specific performance or cost constraints to consider for the embedding generation process?
*   What is the expected volume of data in these tables? This will influence batch sizing and processing time estimates.

## 7. Example Data Structures (for reference)

### `master_sensory_panel_joined_silver` - `data` column example:
```json
{
  "item_spec_number": "5079621-001",
  "item_product_id": "Mini Pepperoni",
  "item_type": "Proposed",
  "item_brand": "Kroger",
  "item_plant_supplier": "JOHN MORRELL & CO.",
  "item_formula_number_code": null,
  "item_upc_number": null,
  "summary_results": null,
  "summary_other_findings": null,
  "summary_special_notes": null,
  "summary_met_expectation": null,
  "summary_panel_pass_fail": null,
  "dept_info_test_methodology": "Sequential Monadic",
  "dept_info_test_methodology_comments": null,
  "dept_info_attributes_factors_to_be_tested": "Appearance,Flavor,Overall Liking,Texture",
  "dept_info_attributes_factors_to_be_tested_comments": null,
  "dept_info_date_of_panel": "9999-12-31",
  "benchmark_product_id": "Mini Pepperoni",
  "benchmark_brand": "Hormel",
  "benchmark_company": "Hormel",
  "benchmark_formula_code": null,
  "benchmark_upc_number": "0002147483647",
  "date_of_formal_cutting": null,
  "ingredient_statement_concept_statement": null,
  "ingredient_statement_curr_benchmark_ing_stat_1": "Pork, Beef, salt, contains or less of water, dextrose, spices, lactic acid started Culture, Oleoresin of Paprika, Garlic Powder, Sodium Nitrite, BHA, BHT, Citric Acid ",
  "ingredient_statement_additional_ingredients": "None",
  "ingredient_statement_gen_product_descrip": "Mini Pepperoni ",
  "ingredient_statement_propose_ingred_stat_2": null,
  "ingredient_statement_curr_benchmark_ing_stat_2": null,
  "ingredient_statement_known_allergens": "None",
  "ingredient_statement_propose_ingred_stat_1": "Pork, Beef, Salt, Contains 2% or less of dextrose, natural flavors, Oleoresin of Paprika, lactic acid started Culture, Dehydrated Garlic, Sodium Nitrite, BHA, BHT, Citric Acid",
  "ingredient_statement_curr_benchmark_ing_stat_3": null,
  "ingredient_statement_propose_ingred_stat_3": null
}
```

### `master_sensory_responses_collected_silver` - `data` column example:
```json
{
  "test_name": "5173816 Chicken and Apple Breakfast Sausage Patty",
  "test_id": "6223",
  "test_completed_date": "2024-10-17",
  "unique_panelist_id": "27264",
  "questions": [
    {
      "sample_set_completion_timestamp": "2024-09-24T14:03:34Z",
      "sample_position": "1",
      "sample_number": "1",
      "sample_name": "Proposed",
      "design_position_name": "Position 1",
      "sample_type": "Sample",
      "question": "appearance_liking",
      "response": "6",
      "timestamp": "2024-09-24T14:01:09Z"
    },
    {
      "sample_set_completion_timestamp": "2024-09-24T14:03:34Z",
      "sample_position": "1",
      "sample_number": "1",
      "sample_name": "Proposed",
      "design_position_name": "Position 1",
      "sample_type": "Sample",
      "question": "overall_flavor_liking",
      "response": "8",
      "timestamp": "2024-09-24T14:01:38Z"
    },
    // ... more questions ...
    {
      "sample_set_completion_timestamp": "2024-09-24T14:03:34Z",
      "sample_position": "1",
      "sample_number": "1",
      "sample_name": "Proposed",
      "design_position_name": "Position 1",
      "sample_type": "Sample",
      "question": "dislike",
      "response": "Apples inside the patty does create a texture that I'm not used to",
      "timestamp": "2024-09-24T14:03:27Z"
    }
  ]
}
```
