from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "pickup_location_id IS NOT NULL",
    "rule 2" : "cancellation_reason_id IS NOT NULL",
    "rule 3" : "dropoff_location_id IS NOT NULL",
    "rule 4" : "pickup_location_id IS NOT NULL"
}

@dp.view(
    name= "Dim_Bookings_Staging"
)

def Dim_Bookings_Staging():
    df_bulk = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_cancel = spark.read.table(
    "realtime_project.silver.map_cancellations_reasons_enr")
    df_booking = (df_bulk.alias("b").join(df_cancel.alias("c"),col("c.cancellation_reason_id") == col("b.cancellation_reason_id"),"left").select(col("b.pickup_location_id"),col("b.dropoff_location_id"),col("b.confirmation_number"),col("b.pickup_city_id"),col("b.dropoff_city_id"),col("b.pickup_address"),col("b.dropoff_address"),col("c.cancellation_reason_id"),col("b.booking_timestamp"),col("b.pickup_timestamp"),col("b.dropoff_timestamp"),col("b.rating"),col("c.cancellation_reason"),col("c.last_updated_timestamp"))).distinct()
    return df_booking


dp.create_streaming_table(name= "Dim_Bookings", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "cancellation_reason_id", 
    }, expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Bookings",
    source = "Dim_Bookings_Staging",
    keys = ["cancellation_reason_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



