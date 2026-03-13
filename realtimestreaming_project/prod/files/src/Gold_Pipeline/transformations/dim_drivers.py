from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "driver_id IS NOT NULL",
    "rule 2" : "driver_name IS NOT NULL"
}

@dp.view(
    name= "Dim_Drivers_Staging"
)

def Dim_Drivers_Staging():
    df_drivers = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_drivers = df_drivers.select(col("driver_id"), col("driver_name"), col("driver_phone"),
    col("driver_rating"), col("driver_license"), col("last_updated_timestamp")).distinct()
    return df_drivers


dp.create_streaming_table(name= "Dim_Drivers", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "driver_id", 
    }, expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Drivers",
    source = "Dim_Drivers_Staging",
    keys = ["driver_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



