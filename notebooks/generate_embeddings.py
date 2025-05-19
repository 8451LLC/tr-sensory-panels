# Databricks notebook source
# MAGIC %pip install openai

# COMMAND ----------

import os, getpass

from delta.tables import DeltaTable
import openai
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t


# Constants
CATALOG = 'manufacturing_dev'
SCHEMA = 'work_agent_barney'
TABLE = 'manufacturing_dev.work_agent_barney.master_sensory_responses_collected_silver'
PK = ['test_id', 'unique_panelist_id']
BATCH_SIZE = 5
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

def fetch_records(table: str, pk: str | list[str], limit: int, data_col='data') -> DataFrame:
    pk = pk if isinstance(pk, list) else [pk]
    return (
        spark.read.table(table)
        .select(*pk, f.to_json(data_col).alias("text"))
        .filter(f.col("data_embedding").isNull())
        .limit(limit)
    )


records_df = fetch_records(TABLE, PK, BATCH_SIZE)
display(records_df)

# COMMAND ----------

def generate_embeddings_df(openai_client: openai.OpenAI, records_df: DataFrame, pk: str | list[str]):
    # Collect the records into memory
    collected = records_df.collect()

    # Extract text for embedding
    texts = [row.text for row in collected]

    # Generate embeddings
    response = openai_client.embeddings.create(model="text-embedding-3-small", input=texts)
    embeddings = [item.embedding for item in response.data]

    # Create new DataFrame
    schema = t.StructType(
        [t.StructField(k, t.StringType(), False) for k in pk] + # assumes pk is a StringType
        [t.StructField("text", t.StringType(), False), t.StructField("data_embedding", t.ArrayType(t.FloatType()), False)]
    )
    data_with_embeddings = [(*[row[pk] for pk in PK], row.text, embedding) for row, embedding in zip(collected, embeddings)]
    embeddings_df = spark.createDataFrame(data_with_embeddings, schema=schema)

    return embeddings_df


embeddings_df = generate_embeddings_df(openai_client, records_df, PK)
display(embeddings_df)

# COMMAND ----------

def merge_embeddings(target_table: str, source_df: DataFrame, pk: str | list[str]) -> None:
    # Merge condition on primary key
    merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in pk])

    # Initialize DeltaTable
    delta_table = DeltaTable.forName(spark, target_table)

    # Merge embeddings into target Delta table
    (
        delta_table.alias("t")
        .merge(source=source_df.alias("s"), condition=merge_condition)
        .whenMatchedUpdate(set={"t.data_embedding": f"s.data_embedding",})
        .execute()
    )

merge_embeddings(TABLE, embeddings_df, PK)

# COMMAND ----------

test = spark.read.table(TABLE)
display(test.filter(f.col("data_embedding").isNotNull()))
