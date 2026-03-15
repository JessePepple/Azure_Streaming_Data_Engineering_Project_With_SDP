from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "cancellation_reason_id IS NOT NULL",
    "rule 2" : "confirmation_number IS NOT NULL"
}
@dp.view(
    name = "Dim_Bookings_stg"
)
def Dim_Bookings_stg():
    df_bookings = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_bookings = df_bookings.select(
    "cancellation_reason_id", "confirmation_number", "pickup_address", "dropoff_address", "cancellation_reason", "base_rate_flag"
    )\
    .withColumn("last_updated_timestamp", current_timestamp())\
        .dropDuplicates(["cancellation_reason_id"])
    return df_bookings

dp.create_streaming_table(name= "Dim_Bookings", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "cancellation_reason_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Bookings",
    source = "Dim_Bookings_stg",
    keys = ["cancellation_reason_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
