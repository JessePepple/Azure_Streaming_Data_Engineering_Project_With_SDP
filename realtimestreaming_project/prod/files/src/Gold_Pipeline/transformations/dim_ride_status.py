from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "ride_status_id IS NOT NULL",
    "rule 2" : "ride_status IS NOT NULL",
}

@dp.view(
    name= "Dim_Ride_Status_Staging"
)

def Dim_Ride_Status_Staging():
    df_bulk = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_ride_status = spark.read.table("realtime_project.silver.map_ride_statuses_enr")
    df_ride_status = (df_bulk.alias("b")
    .join(df_ride_status.alias("r"),col("b.ride_status_id") == col("r.ride_status_id"),"left")
    .select(col("r.ride_status_id"),col("r.ride_status"),col("b.pickup_address"),col("b.dropoff_address"),col("r.last_updated_timestamp"))).distinct()

    return df_ride_status


dp.create_streaming_table(name= "Dim_Ride_Status", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "ride_status_id", 
    },
    expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Ride_Status",
    source = "Dim_Ride_Status_Staging",
    keys = ["ride_status_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



