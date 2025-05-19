"""
Generate OpenAI embeddings for the `data` column in a Databricks table and update the table.
This script is intentionally simple and linear for clarity and maintainability.
"""

import os
import json
import logging
import pandas as pd
from dotenv import load_dotenv

from openai import OpenAI
from sensory.utils.databricks import SQLWarehouse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BATCH_SIZE = 5  # Number of records to process at a time


def serialize_data(val):
    if val is None:
        return ""
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, default=str, sort_keys=True)
            return val
        except json.JSONDecodeError:
            return val
    try:
        return json.dumps(val, default=str, sort_keys=True)
    except Exception:
        return ""


def fetch_records(sql, catalog, schema, table, pk, limit):
    query = f"""
        SELECT {pk}, data
        FROM {catalog}.{schema}.{table}
        WHERE data_embedding IS NULL
        ORDER BY {pk} ASC
        LIMIT {limit}
    """
    rows = sql.query(query)
    return (
        pd.DataFrame(rows, columns=[pk, "data"])
        if rows
        else pd.DataFrame(columns=[pk, "data"])
    )


def generate_embeddings(openai_client, texts):
    response = openai_client.embeddings.create(
        model="text-embedding-3-small", input=texts
    )
    return [item.embedding for item in response.data]


def update_embeddings(sql, df, catalog, schema, table, pk):
    for _, row in df.iterrows():
        emb = row["embedding"]
        if not isinstance(emb, list):
            continue
        inner = ", ".join(str(x) for x in emb)
        emb_sql = f"array({inner})"
        pk_val = row[pk]
        pk_sql = (
            f"'{str(pk_val).replace("'", "''")}'"
            if isinstance(pk_val, str)
            else str(pk_val)
        )
        update = f"""
            UPDATE {catalog}.{schema}.{table}
            SET data_embedding = {emb_sql}
            WHERE {pk} = {pk_sql}
        """
        sql.query(update)


def main():
    load_dotenv()
    sql = SQLWarehouse()
    openai_client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"), base_url=os.getenv("OPENAI_API_BASE")
    )
    catalog = os.getenv("DATABRICKS_CATALOG", "manufacturing_dev")
    schema = os.getenv("DATABRICKS_SCHEMA", "work_agent_barney")
    table = "master_sensory_panel_joined_silver"
    pk = "item_spec_number"

    df = fetch_records(sql, catalog, schema, table, pk, BATCH_SIZE)
    if df.empty:
        logging.info("No records to process.")
        return
    df["text"] = df["data"].apply(serialize_data)
    df["embedding"] = generate_embeddings(openai_client, df["text"].tolist())
    update_embeddings(sql, df, catalog, schema, table, pk)
    logging.info(f"Updated {len(df)} records.")


if __name__ == "__main__":
    main()
