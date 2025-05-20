# Databricks notebook source
# MAGIC %pip install openai ratelimit databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, getpass
import logging

from delta.tables import DeltaTable
import openai
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t
from ratelimit import limits, sleep_and_retry


CATALOG = "manufacturing_dev"
SCHEMA = "work_agent_barney"
OPENAI_API_BASE = "https://api-internal.8451.com/ai/proxy/"

# Rate limits
BATCH_SIZE = 60
CALLS = 10
PERIOD = 60
MAX_CHARS = 20_000

def _set_env(var: str):
    if not os.environ.get(var):
        os.environ[var] = getpass.getpass(f"{var}: ")


# Set env vars
_set_env("OPENAI_API_KEY")
os.environ["OPENAI_API_BASE"] = OPENAI_API_BASE


# Init OpenAI client
openai_client = openai.OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    base_url=os.environ.get("OPENAI_API_BASE"),
)

# COMMAND ----------

def fetch_records(table_name: str, limit: int = None) -> DataFrame:
    df = (
        spark.read.table(table_name)
        .select("id", "data")
        .filter(f.col("data_embedding").isNull())

        # several records exceeding embedding model context window
        .withColumn("data", f.expr(f"substring(data, 1, {MAX_CHARS})"))
    )
    if limit is not None:
        df = df.limit(limit)

    return df


@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def generate_embeddings_rate_limited(data):
    # Call the OpenAI API for embeddings
    response = openai_client.embeddings.create(
        model="text-embedding-3-small", input=data
    )
    return [item.embedding for item in response.data]


def merge_embeddings(target_table: str, embeddings_df: DataFrame) -> None:
    # Initialize DeltaTable
    delta_table = DeltaTable.forName(spark, target_table)

    # Merge embeddings into target Delta table
    (
        delta_table.alias("t")
        .merge(source=embeddings_df.alias("s"), condition="t.id = s.id")
        .whenMatchedUpdate(set={"t.data_embedding": "s.data_embedding"})
        .execute()
    )


def process_and_update_embeddings(target_table: str, max_records: int = None) -> None:
    logging.basicConfig(level=logging.INFO)
    total_processed = 0
    while True:
        # Fetch a batch of records needing embeddings
        records_df = fetch_records(target_table, BATCH_SIZE)
        collected = records_df.collect()
        if not collected:
            logging.info(
                f"No more records to process. Total processed: {total_processed}"
            )
            break  # No more records to process

        # If max_records is set, trim the batch if needed
        if max_records is not None:
            remaining = max_records - total_processed
            if remaining <= 0:
                logging.info(f"Reached max_records limit: {max_records}")
                break
            if len(collected) > remaining:
                collected = collected[:remaining]

        ids = [row.id for row in collected]
        data = [row.data for row in collected]

        logging.info(
            f"Processing batch: {len(collected)} records (total processed so far: {total_processed})"
        )

        # Rate-limited embedding generation
        embeddings = generate_embeddings_rate_limited(data)

        # Prepare DataFrame for merge
        with_embeddings = [(id, embedding) for id, embedding in zip(ids, embeddings)]
        schema = t.StructType(
            [
                t.StructField("id", t.LongType(), False),
                t.StructField("data_embedding", t.ArrayType(t.FloatType()), False),
            ]
        )
        embeddings_df = spark.createDataFrame(with_embeddings, schema=schema)

        # Merge the generated embeddings into the target table
        merge_embeddings(target_table, embeddings_df)

        total_processed += len(collected)
        logging.info(f"Merged batch. Total processed: {total_processed}")
        if max_records is not None and total_processed >= max_records:
            logging.info(f"Reached max_records limit: {max_records}")
            break


# COMMAND ----------

display(
    fetch_records(f"{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver", 60)
    .select(
        'id',
        f.length('data').alias('chars'),
        'data'  
    )
    .orderBy(f.desc('chars'))
)

# COMMAND ----------

process_and_update_embeddings(
    f"{CATALOG}.{SCHEMA}.master_sensory_panel_joined_silver"
)

# COMMAND ----------

panel_joined = (
    spark.read.table(f"{CATALOG}.{SCHEMA}.master_sensory_panel_joined_silver")
    .groupBy(f.col("data_embedding").isNull())
    .count()
)

display(panel_joined)

# COMMAND ----------

process_and_update_embeddings(
    f"{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver"
)

# COMMAND ----------

display(
    fetch_records(f"{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver", 10)
    .select(
        'id',
        f.length('data').alias("length"),
        'data'
    )
)

# COMMAND ----------

responses_collected = (
    spark.read.table(f"{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver")
    .groupBy(f.col("data_embedding").isNull())
    .count()
)

display(responses_collected)
