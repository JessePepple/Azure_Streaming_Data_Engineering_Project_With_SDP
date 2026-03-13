from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "ride_id IS NOT NULL",
    "rule 2" : "passenger_id IS NOT NULL",
    "rule 3" : "driver_id IS NOT NULL",
    "rule 4" : "vehicle_id IS NOT NULL",
    "rule 5" : "vehicle_make_id IS NOT NULL"
}

@dp.view(
    name= "Fact_Rides_Staging"
)

def Fact_Rides_Staging():
    df_bulk = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_fact_rides = df_bulk.select("ride_id","passenger_id","driver_id", "vehicle_id","pickup_location_id","dropoff_location_id","vehicle_type_id","vehicle_make_id","payment_method_id","ride_status_id","pickup_city_id","dropoff_city_id","cancellation_reason_id","pickup_latitude","pickup_longitude","dropoff_longitude","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","subtotal","tip_amount","total_fare", "last_updated_timestamp").distinct()

    return df_fact_rides


dp.create_streaming_table(name= "Fact_Rides", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "ride_id", 
    },
    expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Fact_Rides",
    source = "Fact_Rides_Staging",
    keys = ["ride_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "1",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



