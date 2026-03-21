import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sys.path.append(
    "/Workspace/Users/jessepepple36@gmail.com/Azure_Streaming_Data_Engineering_Project_With_SDP_And_CI-CD/realtimestreaming_project/prod/files/src/Silver_Pipeline/utilities"
   
)

from utils import SilverTransformation

# ---------------------------
# Spark Session Fixture (Databricks-safe)
# ---------------------------
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark


# ---------------------------
# add_cdc_column
# ---------------------------
def test_add_cdc_column(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.add_cdc_column("cdc_ts")

    assert "cdc_ts" in result.columns
    assert result.filter(col("cdc_ts").isNull()).count() == 0


# ---------------------------
# drop_duplicates
# ---------------------------
def test_drop_duplicates(spark):
    data = [("Alice",), ("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.drop_duplicates(["name"])

    assert result.count() == 2


def test_drop_duplicates_no_duplicates(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.drop_duplicates(["name"])

    assert result.count() == 2


# ---------------------------
# fill_all_nullsStr
# ---------------------------
def test_fill_all_nullsStr(spark):
    data = [("Alice",), (None,)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.fill_all_nullsStr(["name"])

    assert result.filter(col("name") == "N/A").count() == 1


def test_fill_all_nullsStr_no_nulls(spark):
    data = [("Alice",), ("Bob",)]
    df = spark.createDataFrame(data, ["name"])

    transformer = SilverTransformation(df)
    result = transformer.fill_all_nullsStr(["name"])

    assert result.filter(col("name") == "N/A").count() == 0


# ---------------------------
# fill_all_nullsInt
# ---------------------------
def test_fill_all_nullsInt(spark):
    data = [(1,), (None,)]
    df = spark.createDataFrame(data, ["age"])

    transformer = SilverTransformation(df)
    result = transformer.fill_all_nullsInt(["age"])

    assert result.filter(col("age") == 0).count() == 1


def test_fill_all_nullsInt_multiple_columns(spark):
    data = [(1, None), (None, 2)]
    df = spark.createDataFrame(data, ["col1", "col2"])

    transformer = SilverTransformation(df)
    result = transformer.fill_all_nullsInt(["col1", "col2"])

    assert result.filter(col("col1") == 0).count() == 1
    assert result.filter(col("col2") == 0).count() == 1


# ---------------------------
# drop_data
# ---------------------------
def test_drop_data(spark):
    data = [("Alice", 25)]
    df = spark.createDataFrame(data, ["name", "age"])

    transformer = SilverTransformation(df)
    result = transformer.drop_data(["age"])

    assert "age" not in result.columns
    assert "name" in result.columns


def test_drop_multiple_columns(spark):
    data = [("Alice", 25, "UK")]
    df = spark.createDataFrame(data, ["name", "age", "country"])

    transformer = SilverTransformation(df)
    result = transformer.drop_data(["age", "country"])

    assert "age" not in result.columns
    assert "country" not in result.columns
