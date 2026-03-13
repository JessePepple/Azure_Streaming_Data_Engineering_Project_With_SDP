from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "passenger_id IS NOT NULL"
}

@dp.view(
    name= "Dim_Passengers_Staging"
)

def Dim_Passengers_Staging():
    df_passengers = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_passengers = df_passengers.select(
        "passenger_id",
        "passenger_name",
        "passenger_email",
        "passenger_phone",
        "last_updated_timestamp"
    ).dropDuplicates(["passenger_id"])

    return df_passengers


dp.create_streaming_table(name= "Dim_Passengers", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "passenger_id", 
    }, 
    expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Passengers",
    source = "Dim_Passengers_Staging",
    keys = ["passenger_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



