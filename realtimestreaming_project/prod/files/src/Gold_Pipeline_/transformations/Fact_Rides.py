from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "ride_id IS NOT NULL",
    "rule 2" : "passenger_id IS NOT NULL",
    "rule 3" : "driver_id IS NOT NULL",
    "rule 4" : "vehicle_id IS NOT NULL",
    "rule 5" : "payment_method_id IS NOT NULL",
    "rule 6" : "ride_status_id IS NOT NULL",
    "rule 7" : "cancellation_reason_id IS NOT NULL"
}
@dp.view(
    name = "Fact_Rides_stg"
)
def Fact_Rides_stg():
    df_rides = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_rides = df_rides.select("ride_id", "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "vehicle_type_id", "vehicle_make_id", "payment_method_id", "ride_status_id", "pickup_city_id", "dropoff_city_id", "cancellation_reason_id", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "subtotal", "tip_amount", "total_fare", "base_rate", "per_mile", "per_minute")\
    .dropDuplicates(["ride_id"])\
    .withColumn("last_updated_timestamp", current_timestamp())
    return df_rides

dp.create_streaming_table(name= "Fact_Rides", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "ride_id"
})

dp.create_auto_cdc_flow(
    target = "Fact_Rides",
    source = "Fact_Rides_stg",
    keys = ["ride_id", "passenger_id", "driver_id", "vehicle_id", "ride_status_id", "cancellation_reason_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
