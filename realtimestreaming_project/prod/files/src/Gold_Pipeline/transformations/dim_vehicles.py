from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "vehicle_type_id IS NOT NULL",
    "rule 2" : "vehicle_make_id IS NOT NULL"
}

@dp.view(
    name= "Dim_Vehicles_Staging"
)

def Dim_Vehicles_Staging():
    df_rides = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_vehicle_types = spark.read.table("realtime_project.silver.map_vehicle_types_enr")
    df_vehicle_makes = spark.read.table("realtime_project.silver.map_vehicle_makes_enr")

    df_vehicles = (df_rides.alias("r")
    .join(df_vehicle_types.alias("vii"),col("r.vehicle_type_id") == col("vii.vehicle_type_id"),"left")
    .join(df_vehicle_makes.alias("vi"), col("r.vehicle_make_id") == col("vi.vehicle_make_id"),"left")
    .select( col("r.vehicle_id"), col("r.vehicle_make_id"), col("vii.vehicle_type"), col("vii.description"), col("vii.vehicle_type_id"), col("r.license_plate"), col("r.vehicle_color"), col("r.vehicle_model"), col("vi.vehicle_make"), col("vii.last_updated_timestamp"))).distinct()

    return df_vehicles


dp.create_streaming_table(name= "Dim_Vehicles", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "vehicle_type_id", 
    },
    expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Vehicles",
    source = "Dim_Vehicles_Staging",
    keys = ["vehicle_type_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)



