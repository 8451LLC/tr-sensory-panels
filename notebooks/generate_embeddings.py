# Databricks notebook source
from pyspark.sql import functions as f
from openai import OpenAI
import os
import json
from dotenv import load_dotenv

CATALOG = "manufacturing_dev"
SCHEMA = "work_agent_barney"
TABLE = "master_sensory_panel_joined_silver"
PK = "item_spec_number"
BATCH_SIZE = 5  # Adjust as needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Merge OpenAI Embeddings in Databricks

# COMMAND ----------


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


# COMMAND ----------


def fetch_records(spark, catalog, schema, table, pk, limit):
    df = spark.sql(
        f"""
        SELECT {pk}, data
        FROM {catalog}.{schema}.{table}
        WHERE data_embedding IS NULL
        ORDER BY {pk} ASC
        LIMIT {limit}
    """
    )
    return df


# COMMAND ----------


def generate_embeddings(openai_client, texts):
    response = openai_client.embeddings.create(
        model="text-embedding-3-small", input=texts
    )
    return [item.embedding for item in response.data]


# COMMAND ----------


def insert_temp_embeddings(spark, df, catalog, schema, temp_table, pk):
    # Drop temp table if exists
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{temp_table}")
    # Write DataFrame to temp table
    (
        df.write.mode("overwrite")
        .format("delta")
        .saveAsTable(f"{catalog}.{schema}.{temp_table}")
    )


# COMMAND ----------


def merge_embeddings(spark, catalog, schema, temp_table, target_table, pk):
    merge_sql = f"""
    MERGE INTO {catalog}.{schema}.{target_table} AS target
    USING {catalog}.{schema}.{temp_table} AS source
    ON target.{pk} = source.{pk}
    WHEN MATCHED THEN UPDATE SET target.data_embedding = source.data_embedding
    """
    spark.sql(merge_sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Embedding Update Logic

# COMMAND ----------

load_dotenv()
openai_client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY"), base_url=os.getenv("OPENAI_API_BASE")
)
catalog = os.getenv("DATABRICKS_CATALOG", CATALOG)
schema = os.getenv("DATABRICKS_SCHEMA", SCHEMA)
table = TABLE
pk = PK
temp_table = "temp_embeddings"


def update_embeddings_inplace(
    spark_session, openai_client, catalog, schema, table, pk, batch_size
):
    # Fetch records with null embedding
    df = (
        spark_session.read.table(f"{catalog}.{schema}.{table}")
        .filter(f.col("data_embedding").isNull())
        .orderBy(pk)
        .limit(batch_size)
    )
    if df.count() == 0:
        print("No records to process.")
        return
    # Serialize data for embedding
    records_pd = df.toPandas()
    records_pd["text"] = records_pd["data"].apply(serialize_data)
    records_pd["embedding"] = generate_embeddings(
        openai_client, records_pd["text"].tolist()
    )
    # Create DataFrame with pk and embedding
    emb_df = spark_session.createDataFrame(records_pd[[pk, "embedding"]])
    # Join and update in place using DataFrame operations
    orig_df = spark_session.read.table(f"{catalog}.{schema}.{table}")
    updated_df = (
        orig_df.alias("orig")
        .join(emb_df.alias("emb"), f.col(f"orig.{pk}") == f.col(f"emb.{pk}"), "left")
        .withColumn(
            "data_embedding",
            f.when(
                f.col("emb.embedding").isNotNull(), f.col("emb.embedding")
            ).otherwise(f.col("orig.data_embedding")),
        )
        .drop(f"emb.{pk}")
        .drop("embedding")
    )
    # Overwrite the table with updated embeddings
    (
        updated_df.write.mode("overwrite")
        .format("delta")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{table}")
    )
    print(f"Updated {len(records_pd)} records in place.")


# Call the function using the Databricks 'spark' session
update_embeddings_inplace(spark, openai_client, catalog, schema, table, pk, BATCH_SIZE)
