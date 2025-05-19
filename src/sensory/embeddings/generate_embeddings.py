# src/sensory/embeddings/generate_embeddings.py
import os
import json
import time
import logging
from typing import List, Dict, Any, Tuple, Optional

try:
    from openai import OpenAI, RateLimitError, APIError
except ImportError:
    print("OpenAI library not installed. Please install with 'pip install openai'")
    # Handle the absence of the library as needed, e.g., by exiting or raising an error
    raise

# Assuming the script is run from the root of the project or PYTHONPATH is set up
try:
    from sensory.utils.databricks import SQLWarehouse
except ImportError:
    print("Could not import SQLWarehouse from sensory.utils.databricks.")
    print("Ensure your PYTHONPATH is set correctly or run from the project root.")
    # Fallback for local testing if structure is different or not installed
    # from ..utils.databricks import SQLWarehouse # If generate_embeddings.py is a module
    raise


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EmbeddingGenerator:
    def __init__(
        self,
        sql_warehouse: SQLWarehouse,
        openai_api_key: str,
        openai_api_base: Optional[str] = None,
    ):
        self.sql_warehouse = sql_warehouse
        self.openai_client = OpenAI(api_key=openai_api_key, base_url=openai_api_base)
        self.embedding_model = "text-embedding-3-small"

        # User-defined limits (halved as requested)
        self.openai_rpm_limit = 500  # Effective requests per minute
        self.openai_tpm_limit = 500000  # Effective tokens per minute

        self.openai_api_batch_size = 20  # Number of texts to send in one OpenAI API call (max 2048 for some models, but smaller is safer for complex inputs)
        self.db_fetch_batch_size = (
            200  # Number of records to fetch from DB at a time for processing
        )
        self.db_update_batch_size = 50  # Number of records to update in DB in a single conceptual batch (actual updates are row-by-row here)

        # Tracking for simplified rate limiting
        self.request_timestamps: List[float] = []
        # Token tracking would require knowing token counts per request, which is complex without a local tokenizer.
        # Relying on OpenAI's client retries and a simple RPM check.

    def _ensure_embedding_column_exists_manually(
        self, full_table_name: str, embedding_column_name: str
    ):
        logging.info(
            f"The script assumes the embedding column '{embedding_column_name}' (e.g., ARRAY<FLOAT> or VECTOR) "
            f"has been manually added to the table '{full_table_name}'."
        )
        logging.info(
            f"Example DDL: ALTER TABLE {full_table_name} ADD COLUMN {embedding_column_name} ARRAY<FLOAT>;"
        )

    def _serialize_data_column(self, data_value: Any) -> str:
        if data_value is None:
            return ""
        try:
            # If data_value is a string that is already JSON (e.g., from some DB drivers)
            if isinstance(data_value, str):
                try:
                    parsed_json = json.loads(data_value)
                    # If it's a list or dict, dump it again for consistent formatting (e.g. sorted keys)
                    # and to handle internal complex types like datetime.
                    if isinstance(parsed_json, (dict, list)):
                        return json.dumps(parsed_json, default=str, sort_keys=True)
                    return data_value  # It was a simple string literal like "foo" or a number as string "123"
                except json.JSONDecodeError:
                    # Not a JSON string, treat as plain text
                    return data_value
            # If it's already a Python dict/list (e.g. Spark connector parsed STRUCT/ARRAY)
            return json.dumps(data_value, default=str, sort_keys=True)
        except Exception as e:
            logging.warning(
                f"Could not serialize data column value: {data_value}. Error: {e}. Returning empty string."
            )
            return ""

    def _get_openai_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        # Simple RPM check
        current_time = time.time()
        self.request_timestamps = [
            t for t in self.request_timestamps if current_time - t < 60
        ]

        if len(self.request_timestamps) >= self.openai_rpm_limit:
            sleep_duration = (
                60.0 - (current_time - self.request_timestamps[0])
                if self.request_timestamps
                else 60.0
            )
            logging.warning(
                f"Approaching RPM limit. Sleeping for {sleep_duration:.2f} seconds."
            )
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            self.request_timestamps = [
                t for t in self.request_timestamps if time.time() - t < 60
            ]  # Re-filter after sleep

        all_embeddings: List[List[float]] = []
        for i in range(0, len(texts), self.openai_api_batch_size):
            sub_batch_texts = texts[i : i + self.openai_api_batch_size]
            if not sub_batch_texts:
                continue

            logging.info(
                f"Requesting embeddings for {len(sub_batch_texts)} texts from OpenAI."
            )
            try:
                response = self.openai_client.embeddings.create(
                    model=self.embedding_model, input=sub_batch_texts
                )
                embeddings_for_sub_batch = [item.embedding for item in response.data]
                all_embeddings.extend(embeddings_for_sub_batch)
                self.request_timestamps.append(time.time())
                # Add a small delay to spread out requests even within a larger batch processing
                time.sleep(0.5)  # 0.5 second delay between API calls
            except RateLimitError as e:
                logging.warning(
                    f"OpenAI rate limit error encountered: {e}. Retrying after delay..."
                )
                time.sleep(
                    e.retry_after if hasattr(e, "retry_after") and e.retry_after else 60
                )
                # Retry the current sub-batch
                try:
                    response = self.openai_client.embeddings.create(
                        model=self.embedding_model, input=sub_batch_texts
                    )
                    embeddings_for_sub_batch = [
                        item.embedding for item in response.data
                    ]
                    all_embeddings.extend(embeddings_for_sub_batch)
                    self.request_timestamps.append(time.time())
                except Exception as retry_e:
                    logging.error(
                        f"Failed to get embeddings for a sub-batch after retry: {retry_e}. Skipping this sub-batch."
                    )
                    all_embeddings.extend(
                        [[]] * len(sub_batch_texts)
                    )  # Add empty embeddings as placeholders
            except APIError as e:
                logging.error(
                    f"OpenAI API error for a sub-batch: {e}. Skipping this sub-batch."
                )
                all_embeddings.extend(
                    [[]] * len(sub_batch_texts)
                )  # Add empty embeddings as placeholders
            except Exception as e:
                logging.error(
                    f"An unexpected error occurred while getting embeddings for a sub-batch: {e}. Skipping this sub-batch."
                )
                all_embeddings.extend([[]] * len(sub_batch_texts))

        return all_embeddings

    def _build_where_clause(
        self, pk_columns: List[str], pk_values: Tuple[Any, ...]
    ) -> str:
        if len(pk_columns) != len(pk_values):
            raise ValueError("Mismatch between number of PK columns and PK values.")

        conditions = []
        for col, val in zip(pk_columns, pk_values):
            if isinstance(val, str):
                conditions.append(
                    f"{col} = '{str(val).replace("'", "''")}'"
                )  # Escape single quotes for SQL
            elif val is None:
                conditions.append(f"{col} IS NULL")
            else:
                conditions.append(f"{col} = {val}")
        return " AND ".join(conditions)

    def process_table(
        self,
        catalog: str,
        schema: str,
        table_name: str,
        pk_columns: List[str],
        data_column: str = "data",
        embedding_column_name: str = "data_embedding",
        overwrite: bool = False,
    ):

        full_table_name = f"{catalog}.{schema}.{table_name}"
        logging.info(f"Starting embedding generation for table: {full_table_name}")
        self._ensure_embedding_column_exists_manually(
            full_table_name, embedding_column_name
        )

        processed_count = 0
        offset = 0  # Used for pagination if overwrite is True

        while True:
            records_to_process_data = []

            if not overwrite:
                # Fetch PKs of records that need embedding
                pk_query_cols = ", ".join(pk_columns)
                idempotency_query = f"""
                SELECT {pk_query_cols} FROM {full_table_name}
                WHERE {embedding_column_name} IS NULL
                ORDER BY {pk_query_cols} 
                LIMIT {self.db_fetch_batch_size} 
                """
                # Note: This LIMIT without OFFSET for non-overwrite means we process in batches
                # until no more NULLs are found.
                logging.info(
                    f"Fetching PKs for up to {self.db_fetch_batch_size} records needing embeddings from {full_table_name}..."
                )
                pk_tuples_to_fetch = self.sql_warehouse.query(idempotency_query)

                if not pk_tuples_to_fetch:
                    logging.info(
                        f"No more records needing embeddings found in {full_table_name}."
                    )
                    break

                logging.info(
                    f"Found {len(pk_tuples_to_fetch)} PKs. Fetching their data..."
                )

                # Construct a query to fetch data for these specific PKs
                # This can be complex for composite keys with many OR conditions.
                # Example for single PK: WHERE pk_col IN (val1, val2, ...)
                # Example for composite: WHERE (pk1=v1a AND pk2=v2a) OR (pk1=v1b AND pk2=v2b) ...

                data_fetch_conditions = " OR ".join(
                    [
                        f"({self._build_where_clause(pk_columns, pk_tuple)})"
                        for pk_tuple in pk_tuples_to_fetch
                    ]
                )
                data_query_cols = ", ".join(pk_columns + [data_column])
                data_fetch_query = f"SELECT {data_query_cols} FROM {full_table_name} WHERE {data_fetch_conditions}"

                records_to_process_data = self.sql_warehouse.query(data_fetch_query)

            else:  # Overwrite mode: paginate through all records
                query_cols = ", ".join(pk_columns + [data_column])
                overwrite_query = f"""
                SELECT {query_cols} FROM {full_table_name}
                ORDER BY {", ".join(pk_columns)}
                LIMIT {self.db_fetch_batch_size} OFFSET {offset}
                """
                logging.info(
                    f"Fetching records (overwrite mode) from {full_table_name}, offset {offset}, limit {self.db_fetch_batch_size}..."
                )
                records_to_process_data = self.sql_warehouse.query(overwrite_query)
                if not records_to_process_data:
                    logging.info(
                        f"No more records found in {full_table_name} for overwrite."
                    )
                    break
                offset += len(records_to_process_data)

            if not records_to_process_data:
                logging.info("No data fetched in this batch.")
                if (
                    not overwrite
                ):  # If non-overwrite and pk_tuples_to_fetch had items, this is an issue.
                    logging.warning(
                        "Fetched PKs but failed to fetch corresponding data. Check data_fetch_query logic."
                    )
                break

            texts_for_embedding = []
            # Store PKs to map embeddings back to records
            # Each item in record_pk_values_map will be a tuple of PK values
            record_pk_values_map: List[Tuple[Any, ...]] = []

            for record_tuple in records_to_process_data:
                # Assumes record_tuple is (pk_val1, ..., pk_valN, data_col_val)
                pk_values = record_tuple[: len(pk_columns)]
                data_val = record_tuple[len(pk_columns)]

                serialized_text = self._serialize_data_column(data_val)
                texts_for_embedding.append(serialized_text)
                record_pk_values_map.append(pk_values)

            if not texts_for_embedding:
                logging.info("No texts to embed in this batch.")
                if (
                    len(records_to_process_data) > 0
                ):  # Should not happen if records_to_process_data was populated
                    logging.warning(
                        "Had records to process, but no texts were generated for embedding."
                    )
                continue  # Next batch

            logging.info(f"Prepared {len(texts_for_embedding)} texts for embedding.")
            generated_embeddings = self._get_openai_embeddings_batch(
                texts_for_embedding
            )

            if len(generated_embeddings) != len(record_pk_values_map):
                logging.error(
                    f"Mismatch in number of embeddings ({len(generated_embeddings)}) "
                    f"and PKs ({len(record_pk_values_map)}). Skipping updates for this batch."
                )
                continue

            logging.info(
                f"Updating {len(generated_embeddings)} records in {full_table_name}..."
            )
            updates_succeeded = 0
            updates_failed = 0

            for i, embedding_vector in enumerate(generated_embeddings):
                if not embedding_vector:  # Skip if embedding failed for this text
                    logging.warning(
                        f"Skipping update for record with PKs {record_pk_values_map[i]} due to empty embedding."
                    )
                    updates_failed += 1
                    continue

                current_pk_values = record_pk_values_map[i]
                where_clause = self._build_where_clause(pk_columns, current_pk_values)

                # Format embedding vector for SQL (e.g., ARRAY[1.0, 2.0, ...])
                embedding_sql_array = (
                    "ARRAY[" + ", ".join(map(str, embedding_vector)) + "]"
                )

                update_query = f"""
                UPDATE {full_table_name}
                SET {embedding_column_name} = {embedding_sql_array}
                WHERE {where_clause}
                """
                try:
                    logging.debug(
                        f"Executing update: {update_query[:200]}..."
                    )  # Log snippet
                    # This is where SQLWarehouse needs to handle DML correctly.
                    self.sql_warehouse.query(update_query)
                    updates_succeeded += 1
                except Exception as e:
                    updates_failed += 1
                    logging.error(
                        f"Failed to update record with PKs {current_pk_values} in {full_table_name}: {e}"
                    )
                    logging.error(
                        f"Failed query (first 200 chars): {update_query[:200]}"
                    )

            processed_count += updates_succeeded
            logging.info(
                f"Batch update summary: {updates_succeeded} succeeded, {updates_failed} failed."
            )

            if (
                not overwrite and not pk_tuples_to_fetch
            ):  # Should have broken earlier if no PKs
                break
            if (
                overwrite and len(records_to_process_data) < self.db_fetch_batch_size
            ):  # Last page in overwrite mode
                break

        logging.info(
            f"Finished embedding generation for {full_table_name}. Total records updated: {processed_count}"
        )


def main():
    from dotenv import load_dotenv

    load_dotenv()

    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_http_path = os.getenv("DATABRICKS_HTTP_PATH")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    openai_api_base = os.getenv("OPENAI_API_BASE")

    if not all(
        [databricks_host, databricks_http_path, databricks_token, openai_api_key]
    ):
        logging.error(
            "Missing critical environment variables (DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, OPENAI_API_KEY)."
        )
        return

    sql_warehouse_instance = SQLWarehouse(
        server_hostname=databricks_host,
        http_path=databricks_http_path,
        access_token=databricks_token,
    )

    generator = EmbeddingGenerator(
        sql_warehouse_instance, openai_api_key, openai_api_base
    )

    catalog_name = os.getenv("DATABRICKS_CATALOG", "manufacturing_dev")
    schema_name = os.getenv("DATABRICKS_SCHEMA", "work_agent_barney")

    # --- IMPORTANT: User must verify these PKs ---
    tables_to_process_config = [
        {
            "table_name": "master_sensory_panel_joined_silver",
            "pk_columns": ["item_spec_number"],  # Please verify
            "data_column": "data",
            "embedding_column": "data_embedding",
        },
        {
            "table_name": "master_sensory_responses_collected_silver",
            "pk_columns": [
                "test_id",
                "unique_panelist_id",
            ],  # Please verify (order might matter for ORDER BY)
            "data_column": "data",
            "embedding_column": "data_embedding",
        },
    ]

    # --- Configuration for overwrite ---
    # Set to True to re-generate and overwrite embeddings for ALL records.
    # Set to False to only generate embeddings for records where the embedding column IS NULL.
    # This could be made a command-line argument.
    OVERWRITE_EXISTING_EMBEDDINGS = False
    # Example: import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--overwrite", action="store_true", help="Overwrite existing embeddings.")
    # args = parser.parse_args()
    # OVERWRITE_EXISTING_EMBEDDINGS = args.overwrite

    logging.info(
        f"Starting embedding generation process. Overwrite mode: {OVERWRITE_EXISTING_EMBEDDINGS}"
    )
    logging.warning(
        "Ensure that the target embedding columns (e.g., 'data_embedding' of type ARRAY<FLOAT>) "
        "have been manually added to the tables before running this script."
    )

    for config in tables_to_process_config:
        logging.info(
            f"Processing table: {config['table_name']} with PKs: {config['pk_columns']}"
        )
        generator.process_table(
            catalog=catalog_name,
            schema=schema_name,
            table_name=config["table_name"],
            pk_columns=config["pk_columns"],
            data_column=config["data_column"],
            embedding_column_name=config["embedding_column"],
            overwrite=OVERWRITE_EXISTING_EMBEDDINGS,
        )

    logging.info("All tables processed.")


if __name__ == "__main__":
    main()
