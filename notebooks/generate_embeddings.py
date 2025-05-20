# Databricks notebook source
# MAGIC %pip install openai databricks-vectorsearch

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, getpass

from delta.tables import DeltaTable
import openai
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t


CATALOG = 'manufacturing_dev'
SCHEMA = 'work_agent_barney'
OPENAI_API_BASE='https://api-internal.8451.com/ai/proxy/'


def _set_env(var: str):
    if not os.environ.get(var):
        os.environ[var] = getpass.getpass(f"{var}: ")


# Set env vars
_set_env('OPENAI_API_KEY')
os.environ['OPENAI_API_BASE'] = OPENAI_API_BASE


# Init OpenAI client
openai_client = openai.OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    base_url=os.environ.get("OPENAI_API_BASE"),
)

# COMMAND ----------

def fetch_records(table_name: str, limit: int) -> DataFrame:
    return (
        spark.read.table(table_name)
        .select('id', 'data')
        .filter(f.col("data_embedding").isNull())
        .limit(limit)
    )


def generate_embeddings(records_df: DataFrame) -> DataFrame:
    # Collect the records into memory
    collected = records_df.collect()

    # Extract text for embedding
    ids = [row.id for row in collected]
    data = [row.data for row in collected]

    # Generate embeddings
    response = openai_client.embeddings.create(model="text-embedding-3-small", input=data)
    embeddings = [item.embedding for item in response.data]

    # Join embeddings with id
    with_embeddings = [(id, embedding) for id, embedding in zip(ids, embeddings)]

    # Create embeddings DataFrame Schema
    schema = t.StructType([
        t.StructField("id", t.LongType(), False),
        t.StructField("data_embedding", t.ArrayType(t.FloatType()), False)
    ])

    # Return embeddings DataFrame
    return spark.createDataFrame(with_embeddings, schema=schema)


def merge_embeddings(target_table: str, embeddings_df: DataFrame) -> None:
    # Initialize DeltaTable
    delta_table = DeltaTable.forName(spark, target_table)

    # Merge embeddings into target Delta table
    (
        delta_table.alias("t")
        .merge(
            source=embeddings_df.alias("s"),
            condition="t.id = s.id"
        )
        .whenMatchedUpdate(set={"t.data_embedding": f"s.data_embedding",})
        .execute()
    )


def insert_embeddings(target_table: str, limit) -> None:
    records_df = fetch_records(target_table, limit)
    embeddings_df = generate_embeddings(records_df)
    merge_embeddings(target_table, embeddings_df)


insert_embeddings(
    # target_table=f'{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver',
    target_table=f'{CATALOG}.{SCHEMA}.master_sensory_panel_joined_silver',
    limit=5
)


# COMMAND ----------

test = (
    spark.read.table(f'{CATALOG}.{SCHEMA}.master_sensory_panel_joined_silver')
    .filter(f.col("data_embedding").isNotNull())
)

display(test)

# COMMAND ----------

# from databricks.vector_search.client import VectorSearchClient


# client = VectorSearchClient()
# index = client.create_endpoint_and_wait(
#     name="master_sensory_data_endpoint",
#     endpoint_type="STANDARD"
# )

# COMMAND ----------

display(test)

# COMMAND ----------

client = VectorSearchClient()

index = client.create_delta_sync_index_and_wait(
    endpoint_name='master_sensory_data_endpoint',
    index_name=f'{CATALOG}.{SCHEMA}.master_sensory_test_index',
    primary_key='test_id',
    source_table_name=f"{CATALOG}.{SCHEMA}.master_sensory_test_embeddings",
    pipeline_type="TRIGGERED",
    embedding_dimension=1536,
    embedding_vector_column="data_embedding",
    verbose=True
)

# COMMAND ----------


