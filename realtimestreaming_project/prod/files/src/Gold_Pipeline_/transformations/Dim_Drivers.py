from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "driver_id IS NOT NULL",
    "rule 2" : "driver_name IS NOT NULL"
}
@dp.view(
    name = "Dim_Drivers_stg"
)
def Dim_Drivers_stg():
    df_drivers = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_drivers = df_drivers.select(
    "driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license", "rating"
).dropDuplicates(["driver_id"])\
     .withColumn("last_updated_timestamp", current_timestamp())
    return df_drivers

dp.create_streaming_table(name= "Dim_Drivers", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "driver_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Drivers",
    source = "Dim_Drivers_stg",
    keys = ["driver_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
