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
    name= "map_ride_status_stg"
)
def map_ride_status_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Map_Ride_Statuses_folder/Map_Ride_Statuses_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Ride_Statuses_Staging")
    return df

@dp.view(
    name="map_ride_statuses_transformation"
)
def map_ride_statuses_transformation():
    df_statuses = spark.readStream.table("map_ride_status_stg")
    Map_Ride_Statuses = SilverTransformation(df_statuses) 
    df_statuses = Map_Ride_Statuses.add_cdc_column("last_updated_timestamp")
    df_statuses = Map_Ride_Statuses.drop_data("_rescued_data")
    return df_statuses


dp.create_streaming_table(name="map_ride_statuses_enr",comment= "Merge For Silver Layer Data", table_properties={
        "pipelines.autoOptimize.zOrderCols": "ride_status_id"
    })

dp.create_auto_cdc_flow(
    target = "map_ride_statuses_enr",
    source = "map_ride_statuses_transformation",
    keys = ["ride_status_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_Map_Ride_to_adls():
    df = spark.read.table("realtime_project.silver.map_ride_statuses_enr")

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Map_Ride_Statuses_folder/Map_Ride_Statuses_Data")
    )
    return df



