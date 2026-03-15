from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "ride_status_id IS NOT NULL",
    "rule 2" : "pickup_location_id IS NOT NULL",
    "rule 3" : "dropoff_location_id IS NOT NULL",
    "rule 4" : "pickup_city_id IS NOT NULL",
    "rule 5" : "dropoff_city_id IS NOT NULL"
}
@dp.view(
    name = "Dim_Rides_Status_stg"
)
def Dim_Rides_Status_stg():
    df_ride_status = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_ride_status = df_ride_status.select(
    "ride_status_id", "ride_status","pickup_location_id", "dropoff_location_id","pickup_city_id", "dropoff_city_id", "pickup_address", "dropoff_address", "booking_timestamp", "pickup_timestamp", "dropoff_timestamp", 
).dropDuplicates(["ride_status_id"])\
    .withColumn("last_updated_timestamp", current_timestamp())
    return df_ride_status

dp.create_streaming_table(name= "Dim_Ride_Status", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "rides_status_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Ride_Status",
    source = "Dim_Rides_Status_stg",
    keys = ["ride_status_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
