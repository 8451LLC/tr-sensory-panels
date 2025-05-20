# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
import pyspark.sql.types as t


CATALOG = "manufacturing_dev"
SCHEMA = "work_agent_barney"
CSV_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/compusense/csv_questionnaire/compusense_extract.csv"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build bronze table

# COMMAND ----------


def load_responses_from_csv() -> DataFrame:
    """
    Load questionaire responses from CSV.
    """
    responses = spark.read.csv(CSV_PATH, header=True, inferSchema=False)
    return responses


def filter_rows(responses: DataFrame) -> DataFrame:
    """
    Filter out errant rows.
    """
    # Filter out all null rows
    filtered = responses.dropna(how="all")

    # Filter out repeated headers
    filtered = filtered.filter(~f.col("Test_Name").contains("Test_Name"))
    return filtered


def clean_cols(responses: DataFrame) -> DataFrame:
    """
    Clean responses column names.
    """

    def clean_col_name(col_name: str) -> str:
        cleaned = (
            # remove unallowed chars
            col_name.replace(" ", "_")
            .replace(",", "_")
            .replace(";", "_")
            .replace("{", "_")
            .replace("}", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("\n", "_")
            .replace("\t", "_")
            .replace("=", "_")
            # lower case
            .lower()
        )

        # remove '__'
        while "__" in cleaned:
            cleaned = cleaned.replace("__", "_")

        return cleaned

    clean_cols = [clean_col_name(col) for col in responses.columns]
    return responses.toDF(*clean_cols)


def write_responses_bronze() -> None:
    """
    Load, clean, and write reponse to a table.
    """
    (
        load_responses_from_csv()
        .transform(filter_rows)
        .transform(clean_cols)
        .write.mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.master_sensory_responses_bronze")
    )


# Write bronze table
write_responses_bronze()

# COMMAND ----------

responses_bronze = spark.read.table(
    f"{CATALOG}.{SCHEMA}.master_sensory_responses_bronze"
)
display(responses_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build long table

# COMMAND ----------


def unpivot_questions(responses_bronze: DataFrame) -> DataFrame:
    """
    Pivot responses to long format.
    """
    # Identify the last id vars
    panelist_id_index = responses_bronze.columns.index("unique_panelist_id")
    id_vars = responses_bronze.columns[: panelist_id_index + 1]

    # Identify the question cols to pivot
    question_cols = responses_bronze.columns[panelist_id_index + 1 :]

    # Melt, i.e. unpivot, the question cols
    return (
        responses_bronze.melt(
            ids=id_vars,
            values=question_cols,
            variableColumnName="question",
            valueColumnName="response",
        )
        # Filter out null responses
        .filter(f.col("response").isNotNull())
    )


def pivot_timestamp_cols(responses_long: DataFrame) -> DataFrame:
    """
    Pivot timestamp cols. There might be a more performant way to achieve
    the desired outcome because the timestamp columns were already wide
    before. However, this is logically simpler.
    """
    # Filter for timestamp rows and identify the question_stem
    timestamps: DataFrame = responses_long.filter(
        f.col("question").rlike(r"^.*time_stamp$")
    ).select(
        "test_id",
        "unique_panelist_id",
        "sample_number",
        f.regexp_replace("question", r"_time_stamp$", "").alias("question_stem"),
        f.col("response").alias("timestamp"),
    )

    # Filter for responses (non-timestamp) rows.
    responses: Dataframe = responses_long.filter(
        f.col("question").rlike(r"^((?!time_stamp).)*$")
    )

    # Join the timestamp cols with the non-timestamp cols.
    return (
        responses.alias("r")
        .join(
            timestamps.alias("t"),
            [
                f.col("r.test_id") == f.col("t.test_id"),
                f.col("r.unique_panelist_id") == f.col("t.unique_panelist_id"),
                f.col("r.sample_number") == f.col("t.sample_number"),
                # join on question starts with question_stem from timestamps
                f.col("r.question").startswith(f.col("t.question_stem")),
            ],
            how="left",
        )
        # drop timestamp cols we don't need
        .drop(
            f.col("t.test_id"),
            f.col("t.unique_panelist_id"),
            f.col("t.sample_number"),
            f.col("t.question_stem"),
        )
    )


def write_responses_long_bronze(responses_bronze: DataFrame) -> None:
    """
    Unpivot wide bronze table to long format and write to bronze table.
    This could be considered silver, but arguably still pretty raw.
    """
    (
        responses_bronze
        .transform(unpivot_questions)
        .transform(pivot_timestamp_cols)
        .write.mode("overwrite")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.master_sensory_responses_long_bronze")
    )


# Write long bronze table
write_responses_long_bronze(responses_bronze)

# COMMAND ----------

responses_long_bronze = spark.read.table(
    f"{CATALOG}.{SCHEMA}.master_sensory_responses_long_bronze"
)
display(responses_long_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect responses

# COMMAND ----------

def collect_responses(responses_long_bronze: DataFrame) -> DataFrame:
    """
    Collect responses for each test_id, unique_participant_id into an array of struct.
    """

    def sort_questions_by_timestamp(x, y: t.StructType):
        return f.when(x["timestamp"].isNotNull(), x["timestamp"].cast("int")).otherwise(
            0
        ) - f.when(y["timestamp"].isNotNull(), y["timestamp"].cast("int")).otherwise(0)

    return (
        responses_long_bronze
        # filter for test_id not null and unique_panelist_id not null
        .filter(f.col("test_id").isNotNull() & f.col("unique_panelist_id").isNotNull())
        .select(
            "test_name",
            "test_id",
            f.to_date(f.col("test_completed_date"), "M/d/yyyy").alias(
                "test_completed_date"
            ),
            "unique_panelist_id",
            # Build struct of question
            f.struct(
                # 'test_name',
                # 'test_id',
                # 'test_completed_date',
                # 'test_section_types',
                # 'section_name',
                # 'section_number',
                # 'sample_set_number',
                f.to_timestamp(
                    "sample_set_completion_date", "M/d/yyyy h:mm:ss a"
                ).alias("sample_set_completion_timestamp"),
                # 'rep',
                # 'rep_name',
                "sample_position",
                "sample_number",
                "sample_name",
                "design_position_name",
                # 'blinding_code',
                "sample_type",
                # 'unique_panelist_id',
                "question",
                "response",
                f.to_timestamp("timestamp", "M/d/yyyy h:mm:ss a").alias("timestamp"),
            ).alias("question"),
        )
        .groupBy(
            "test_name",
            "test_id",
            "test_completed_date",
            "unique_panelist_id",
        )
        # Collect list of quesion structs
        .agg(f.collect_list("question").alias("questions"))
        # sort questions array by timestamp value in struct
        .withColumn("questions", f.array_sort("questions", sort_questions_by_timestamp))
        # gather entire row into one struct
        .select(
            "test_name",
            "test_id",
            "test_completed_date",
            "unique_panelist_id",
            f.to_json(f.struct("*")).alias("data"),
        )
    )


def write_responses_collected_silver(responses_long_bronze: DataFrame) -> None:
    """
    Write responses collected into silver table. The resulting 'data' column will
    be suitable for LLM summary and/or embedding.
    """
    # Create the table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            test_name STRING,
            test_id STRING NOT NULL,
            test_completed_date DATE,
            unique_panelist_id STRING NOT NULL,
            data STRING,
            data_embedding ARRAY<FLOAT>,
            PRIMARY KEY (id)
        ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Transform the responses
    responses_collected = responses_long_bronze.transform(collect_responses)

    # Create temp view
    responses_collected.createOrReplaceTempView("responses_collected")

    # Insert data into the table
    spark.sql(f"""
        INSERT OVERWRITE {CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver
        BY NAME
        SELECT
            *
        FROM
            responses_collected
    """)


write_responses_collected_silver(responses_long_bronze)

# COMMAND ----------

responses_collected_silver = spark.read.table(
    f"{CATALOG}.{SCHEMA}.master_sensory_responses_collected_silver"
)
display(
    responses_collected_silver
    .orderBy(
        f.desc("test_completed_date"),
        "test_id",
        "unique_panelist_id",
    )
)
