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
    name= "bulk_rides_stg"
)
def bulk_rides_stg():
    df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.inferColumnTypes", "true").option("cloudFiles.schemaLocation","abfss://silver@realtimejess.dfs.core.windows.net/Bulk_Rides_folder/Bulk_Rides_checkpointLocation").option("cloudFiles.schemaEvolutionMode", "addNewColumns").load("abfss://bronze@realtimejess.dfs.core.windows.net/Bulk_Rides_Staging")
    return df

@dp.view(
    name="bulk_rides_transformation"
)
def bulk_rides_transformation():

    df_bulk_rides = spark.readStream.table("bulk_rides_stg")

    Bulk_Rides = SilverTransformation(df_bulk_rides)

    df_bulk_rides = Bulk_Rides.add_cdc_column("last_updated_timestamp")
    df_bulk_rides = Bulk_Rides.drop_duplicates(["ride_id"])

    df_bulk_rides = Bulk_Rides.fill_all_nullsStr([
        "confirmation_number","passenger_id","driver_id","vehicle_id",
        "pickup_location_id","dropoff_location_id","passenger_name",
        "passenger_email","passenger_phone","driver_name","driver_phone",
        "driver_license","vehicle_model","vehicle_color","license_plate",
        "pickup_address","dropoff_address","booking_timestamp",
        "pickup_timestamp","dropoff_timestamp","rating"
    ])

    df_bulk_rides = Bulk_Rides.fill_all_nullsInt([
        "tip_amount","duration_minutes","time_fare","distance_fare",
        "pickup_city_id","ride_status_id","dropoff_city_id",
        "cancellation_reason_id","vehicle_type_id","vehicle_make_id",
        "payment_method_id"
    ])
    df_bulk_rides = Bulk_Rides.drop_data("_rescued_data")
    return df_bulk_rides

dp.create_streaming_table(name="bulk_rides_enr",comment= "Merge For Silver Layer Data",
table_properties={
        "pipelines.autoOptimize.zOrderCols": "ride_id"
    })

dp.create_auto_cdc_flow(
    target = "bulk_rides_enr",
    source = "bulk_rides_transformation",
    keys = ["ride_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_silver_to_adls():
    df = spark.read.table("realtime_project.silver.bulk_rides_enr")

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Bulk_Rides_folder/Bulk_Rides_Data")
    )
    return df



