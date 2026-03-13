from pyspark import pipelines as dp
from pyspark.sql.functions import *

class SilverTransformation:

    def __init__(self, df=None):
        self.df = df

    def add_cdc_column(self, cdc_column):
        self.df = self.df.withColumn(cdc_column, current_timestamp())
        return self.df

    def drop_duplicates(self, specific_column):
        self.df = self.df.dropDuplicates(specific_column)
        return self.df

    def fill_all_nullsStr(self, col_names):
        for c in col_names:
            self.df = self.df.fillna({c: "N/A"})
        return self.df

    def fill_all_nullsInt(self, col_names):
        for c in col_names:
            self.df = self.df.fillna({c: 0})
        return self.df

    def drop_data(self, cols):
        self.df = self.df.drop(*cols)
        return self.df


@dp.table(
    name= "map_cancellations_stg"
)
def map_cancellations_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Map_Cancellations_folder/Map_Cancellations_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Cancellation_Reasons_Staging")
    return df

@dp.view(
    name="map_cancellations_reasons_transformation"
)
def map_payments_transformation():
    df_cancellations = spark.readStream.table("map_cancellations_stg")
    Map = SilverTransformation(df_cancellations)
    df_cancellations = Map.drop_duplicates(["cancellation_reason_id"])
    df_cancellations = Map.fill_all_nullsStr(["cancellation_reason"])
    df_cancellations = Map.add_cdc_column("last_updated_timestamp")
    df_cancellations = Map.drop_data("_rescued_data")
    
    return df_cancellations


dp.create_streaming_table(name="map_cancellations_reasons_enr",comment= "Merge For Silver Layer Data", table_properties={
        "pipelines.autoOptimize.zOrderCols": "cancellation_reason_id"
    })

dp.create_auto_cdc_flow(
    target = "map_cancellations_reasons_enr",
    source = "map_cancellations_reasons_transformation",
    keys = ["cancellation_reason_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_Map_Cancellations_to_adls():
    df = spark.read.table("realtime_project.silver.map_cancellations_reasons_enr")

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Map_Cancellations_folder/Map_Cancellations_Data")
    )
    return df
