# src/sensory/embeddings/generate_embeddings.py
import os
import json
import logging
import pandas as pd
from dotenv import load_dotenv
from typing import Optional, Any, List

try:
    from openai import OpenAI
except ImportError:
    print("OpenAI library not installed. Please install with 'pip install openai'")
    raise

try:
    from sensory.utils.databricks import SQLWarehouse
except ImportError:
    print("Could not import SQLWarehouse from sensory.utils.databricks.")
    print("Ensure your PYTHONPATH is set correctly or run from the project root.")
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

    def _serialize_data_column(self, data_value: Any) -> str:
        """
        Serializes a data column to a JSON string.
        Matches notebook: json.dumps(x, default=str, sort_keys=True) if not isinstance(x, str) else x
        """
        if data_value is None:
            return ""
        if isinstance(data_value, str):
            try:
                parsed_json = json.loads(data_value)
                if isinstance(parsed_json, (dict, list)):
                    return json.dumps(parsed_json, default=str, sort_keys=True)
                return data_value  # It was a simple string literal
            except json.JSONDecodeError:
                return data_value  # Not a JSON string, treat as plain text
        try:
            return json.dumps(data_value, default=str, sort_keys=True)
        except Exception as e:
            logging.warning(
                f"Could not serialize data value: {str(data_value)[:50]}. Error: {e}. Returning empty str."
            )
            return ""

    def fetch_records_to_embed(
        self, catalog: str, schema: str, table: str, pk_column: str, limit: int = 5
    ) -> pd.DataFrame:
        """Fetches records that need embedding."""
        logging.info(
            f"Fetching up to {limit} records from {catalog}.{schema}.{table} "
            f"where data_embedding is NULL."
        )
        query = f"""
            SELECT {pk_column}, data
            FROM {catalog}.{schema}.{table}
            WHERE data_embedding IS NULL
            ORDER BY {pk_column} ASC
            LIMIT {limit}
        """
        try:
            rows = self.sql_warehouse.query(query)
            if not rows:
                logging.info("No records found needing embedding.")
                return pd.DataFrame(columns=[pk_column, "data"])
            df = pd.DataFrame(rows, columns=[pk_column, "data"])
            logging.info(f"Fetched {len(df)} records.")
            return df
        except Exception as e:
            logging.error(f"Error fetching records: {e}")
            return pd.DataFrame(columns=[pk_column, "data"])

    def generate_embeddings_for_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generates embeddings for the 'data' column of a DataFrame."""
        if df.empty or "data" not in df.columns:
            logging.info(
                "DataFrame is empty or missing 'data' column. No embeddings to generate."
            )
            return df

        texts = df["data"].apply(self._serialize_data_column).tolist()

        if not texts:
            logging.info("No texts to embed after serialization.")
            df["embedding"] = None  # Ensure column exists
            return df

        logging.info(f"Requesting embeddings for {len(texts)} texts from OpenAI.")
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model, input=texts
            )
            embeddings = [item.embedding for item in response.data]
            df["embedding"] = embeddings
            logging.info(f"Successfully generated {len(embeddings)} embeddings.")
        except Exception as e:
            logging.error(
                f"OpenAI API error: {e}. Setting embeddings to None for this batch."
            )
            df["embedding"] = [None] * len(df)
        return df

    def update_records_with_embeddings(
        self, df: pd.DataFrame, catalog: str, schema: str, table: str, pk_column: str
    ):
        """Updates records in the database with generated embeddings."""
        if df.empty or "embedding" not in df.columns or pk_column not in df.columns:
            logging.info(
                "DataFrame is empty or missing 'embedding' or PK column. No records to update."
            )
            return

        updated_count = 0
        for _, row in df.iterrows():
            embedding_list = row["embedding"]
            pk_val = row[pk_column]

            if embedding_list is None or not isinstance(embedding_list, list):
                logging.warning(
                    f"Skipping update for PK {pk_val}: embedding is missing or not a list."
                )
                continue

            update_query = "N/A"  # Initialize for logging
            try:
                json_array_content = json.dumps(embedding_list)
                inner_values = json_array_content[1:-1]
                emb_sql_format = f"array({inner_values})"

                pk_val_sql = (
                    f"'{str(pk_val).replace("'", "''")}'"
                    if isinstance(pk_val, str)
                    else str(pk_val)
                )

                update_query = f"""
                    UPDATE {catalog}.{schema}.{table}
                    SET data_embedding = {emb_sql_format}
                    WHERE {pk_column} = {pk_val_sql}
                """
                logging.debug(
                    f"Executing update for PK {pk_val}: {update_query[:200]}..."
                )
                self.sql_warehouse.query(update_query)
                updated_count += 1
            except Exception as e:
                logging.error(
                    f"Failed to update PK {pk_val} in {catalog}.{schema}.{table}: {e}"
                )
                logging.error(f"Failed query snippet: {update_query[:200]}")

        logging.info(
            f"Attempted to update {len(df)} records. Successfully updated: {updated_count}."
        )


def main():
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
            "Missing critical env vars. Ensure DATABRICKS_HOST, DATABRICKS_HTTP_PATH, "
            "DATABRICKS_TOKEN, and OPENAI_API_KEY are set."
        )
        return

    try:
        sql_warehouse_instance = SQLWarehouse(
            server_hostname=databricks_host,
            http_path=databricks_http_path,
            access_token=databricks_token,
        )
        logging.info("Successfully connected to Databricks SQL Warehouse.")
    except Exception as e:
        logging.error(f"Failed to initialize SQLWarehouse: {e}")
        return

    generator = EmbeddingGenerator(
        sql_warehouse_instance, openai_api_key, openai_api_base
    )

    catalog_name = os.getenv("DATABRICKS_CATALOG", "manufacturing_dev")
    schema_name = os.getenv("DATABRICKS_SCHEMA", "work_agent_barney")
    table_name = "master_sensory_panel_joined_silver"
    pk_column_name = "item_spec_number"

    logging.info(
        f"Processing table: {catalog_name}.{schema_name}.{table_name} with PK: {pk_column_name}"
    )

    records_df = generator.fetch_records_to_embed(
        catalog_name, schema_name, table_name, pk_column_name, limit=5
    )

    if records_df.empty:
        logging.info("No records to process.")
        return

    df_with_embeddings = generator.generate_embeddings_for_df(records_df)
    df_to_update = df_with_embeddings.dropna(subset=["embedding"])

    if df_to_update.empty:
        logging.info("No records with successful embeddings to update.")
        return

    generator.update_records_with_embeddings(
        df_to_update, catalog_name, schema_name, table_name, pk_column_name
    )

    logging.info("Embedding generation process complete for the batch.")


if __name__ == "__main__":
    main()
