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
    name= "map_vehicle_types_stg"
)
def map_vehicle_types_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Map_Vehicle_Types_folder/Map_Vehicle_Types_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Vehicle_Types_Staging")
    return df

@dp.view(
    name="map_vehicle_types_transformation"
)
def map_vehicle_types_transformation():
    df_vehicle_type = spark.readStream.table("map_vehicle_types_stg")
    Map_Vehicle_Type = SilverTransformation(df_vehicle_type)
    df_vehicle_type = Map_Vehicle_Type.add_cdc_column("last_updated_timestamp")
    df_vehicle_type = df_vehicle_type.withColumn("base_rate_flag", when(col("base_rate")>=3, "High Booking").otherwise("Standard Booking"))
    df_vehcile_type = Map_Vehicle_Type.drop_data("_rescued_data")
    return df_vehicle_type


dp.create_streaming_table(name="map_vehicle_types_enr",comment= "Merge For Silver Layer Data", table_properties={
        "pipelines.autoOptimize.zOrderCols": "vehicle_type_id"
    })

dp.create_auto_cdc_flow(
    target = "map_vehicle_types_enr",
    source = "map_vehicle_types_transformation",
    keys = ["vehicle_type_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_Map_Vehicle_Type_to_adls():
    df = spark.read.table("realtime_project.silver.map_vehicle_types_enr")

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Map_Vehicle_Types_folder/Map_Vehicle_Types_Data")
    )
    return df



