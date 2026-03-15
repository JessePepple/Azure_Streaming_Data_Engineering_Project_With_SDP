
from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "vehicle_id IS NOT NULL",
    "rule 2" : "vehicle_type_id IS NOT NULL",
    "rule 3" : "vehicle_make_id IS NOT NULL"
}
@dp.view(
    name = "Dim_Vehicles_stg"
)
def Dim_Vehicles_stg():
    df_vehicles = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_vehicles = df_vehicles.select(
    "vehicle_id", "vehicle_type_id", "vehicle_make_id", "vehicle_model", "vehicle_color", "license_plate", "vehicle_type", "description", "vehicle_make"
    ).dropDuplicates(["vehicle_id"])\
     .withColumn("last_updated_timestamp", current_timestamp())
    return df_vehicles

dp.create_streaming_table(name= "Dim_Vehicles", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "vehicle_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Vehicles",
    source = "Dim_Vehicles_stg",
    keys = ["vehicle_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
