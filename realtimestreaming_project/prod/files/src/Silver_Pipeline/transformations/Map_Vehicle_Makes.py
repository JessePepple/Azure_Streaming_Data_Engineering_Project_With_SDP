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
    name= "map_vehicle_make_stg"
)
def map_vehicle_make_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Map_Vehicle_Makes_folder/Map_Vehicle_Makes_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Vehicle_Makes_Staging")
    return df

@dp.view(
    name="map_vehicle_makes_transformation"
)
def map_vehicle_makes_transformation():
    df_vehicles = spark.readStream.table("map_vehicle_make_stg")
    Map_Vehicle = SilverTransformation(df_vehicles) 
    df_vehicles = Map_Vehicle.add_cdc_column("last_updated_timestamp")
    df_vehicles = Map_Vehicle.drop_data("_rescued_data")
    return df_vehicles


dp.create_streaming_table(name="map_vehicle_makes_enr",comment= "Merge For Silver Layer Data", table_properties={
        "pipelines.autoOptimize.zOrderCols": "vehicle_make_id"
    })

dp.create_auto_cdc_flow(
    target = "map_vehicle_makes_enr",
    source = "map_vehicle_makes_transformation",
    keys = ["vehicle_make_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_Map_Vehicle_to_adls():
    df = spark.read.table("realtime_project.silver.map_vehicle_makes_enr")

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Map_Vehicle_Makes_folder/Map_Vehicle_Makes_Data")
    )
    return df



