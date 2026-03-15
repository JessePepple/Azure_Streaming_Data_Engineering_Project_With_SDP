
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
    df_map_cancellations = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Cancellation_Reasons_Staging")
    df_map_cities = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Cities_Staging")
    df_map_ride_status = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Ride_Statuses_Staging")
    df_map_payments = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Payment_Methods_Staging")
    df_map_vehicles = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Vehicle_Makes_Staging")
    df_map_vehicle_types = spark.read.format("parquet").option("inferSchema", "true").load("abfss://bronze@realtimejess.dfs.core.windows.net/Map_Vehicle_Types_Staging")

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

    df_bulk_rides = df_bulk_rides.alias("r").join(df_map_cancellations.alias("m"), col("r.cancellation_reason_id") == col("m.cancellation_reason_id"), "left")\
   .join(df_map_payments.alias("p"), col("r.payment_method_id") == col ("p.payment_method_id"), "left")\
    .join(df_map_vehicle_types.alias("mv"), col("r.vehicle_type_id")== col("mv.vehicle_type_id"), "left")\
    .join(df_map_vehicles.alias("v"), col("r.vehicle_make_id")== col("v.vehicle_make_id"), "left")\
    .join(df_map_ride_status.alias("rs"), col("r.ride_status_id") == col("rs.ride_status_id"), "left")\
    .select("r.*", "m.cancellation_reason", "p.payment_method", "p.is_card", "p.requires_auth", "mv.vehicle_type", "mv.description", "mv.base_rate", "mv.per_mile", "mv.per_minute", "v.vehicle_make", "rs.ride_status", "rs.is_completed") 
    df_bulk_rides = df_bulk_rides.withColumn("base_rate_flag", when(col("base_rate")>=3, "High Booking").otherwise("Standard_Booking"))
    
    return df_bulk_rides

dp.create_streaming_table(name="bulk_rides_obt",comment= "Merge For Silver Layer Data",
table_properties={
        "pipelines.autoOptimize.zOrderCols": "ride_id"
    })

dp.create_auto_cdc_flow(
    target = "bulk_rides_obt",
    source = "bulk_rides_transformation",
    keys = ["ride_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1"
)


@dp.view()
def write_silver_to_adls():
    df = spark.read.table("realtime_project.silver.bulk_rides_obt")

    (df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save("abfss://silver@realtimejess.dfs.core.windows.net/"
              "Bulk_Rides_OBT/Bulk_Rides_OBT_Data")
    )
    return df