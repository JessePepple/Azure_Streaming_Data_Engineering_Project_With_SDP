from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "passenger_id IS NOT NULL",
    "rule 2" : "passenger_name IS NOT NULL"
}
@dp.view(
    name = "Dim_Passenger_stg"
)
def Dim_Passenger_stg():
    df_passengers = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_passengers = df_passengers.select(
        "passenger_id",
        "passenger_name",
        "passenger_email",
        "passenger_phone",
    ).dropDuplicates(["passenger_id"])\
     .withColumn("last_updated_timestamp", current_timestamp())
    return df_passengers

dp.create_streaming_table(name= "Dim_Passenger", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "passenger_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Passenger",
    source = "Dim_Passenger_stg",
    keys = ["passenger_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
