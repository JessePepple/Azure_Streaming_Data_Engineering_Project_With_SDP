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
    name= "map_payments_stg"
)
def map_payments_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Map_Payment_Methods_folder/Map_Payment_Methods_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Payment_Methods_Staging")
    return df

@dp.view(
    name="map_payments_transformation"
)
def map_payments_transformation():
    df_mappayments = spark.readStream.table("map_payments_stg")
    Map_Payments = SilverTransformation(df_mappayments)
    df_mappayments = Map_Payments.add_cdc_column("last_updated_timestamp")
    df_mappayments = Map_Payments.drop_duplicates(["payment_method_id"])
    
    return df_mappayments


dp.create_streaming_table(name="map_payment_method_enr",comment= "Merge For Silver Layer Data", table_properties={
        "pipelines.autoOptimize.zOrderCols": "payment_method_id"
    })

dp.create_auto_cdc_flow(
    target = "map_payment_method_enr",
    source = "map_payments_transformation",
    keys = ["payment_method_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_Map_Payment_Methods_to_adls():
    df = spark.read.table("realtime_project.silver.map_payment_method_enr")

    (df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Map_Payment_Methods_folder/Map_Payment_Methods_Data")
    )
    return df

