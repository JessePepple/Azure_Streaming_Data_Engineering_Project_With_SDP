from pyspark.sql.functions import *
from pyspark import pipelines as dp


expectations = {
    "rule 1" : "payment_method_id IS NOT NULL",
    "rule 2" : "is_card IS NOT NULL"
}

@dp.view(
    name= "Dim_Payment_Methods_Staging"
)

def Dim_Payment_Methods_Staging():
    df_bulk = spark.readStream.table("realtime_project.silver.bulk_rides_enr")
    df_payment = spark.read.table("realtime_project.silver.map_payment_method_enr")

    df_booking = (df_bulk.alias("b")
    .join(df_payment.alias("p"),col("b.payment_method_id") == col("p.payment_method_id"),
        "left")
    .select(col("b.payment_method_id"),col("p.payment_method"),col("p.is_card"),col("p.requires_auth"),col("p.last_updated_timestamp"))).distinct()
    return df_booking


dp.create_streaming_table(name= "Dim_Payment_Methods", comment= "empty streaming table for SCD TYPE 2", 
    table_properties={
        "pipelines.autoOptimize.zOrderCols": "payment_method_id", 
    },
    expect_all_or_fail=expectations
)

dp.create_auto_cdc_flow(
    target = "Dim_Payment_Methods",
    source = "Dim_Payment_Methods_Staging",
    keys = ["payment_method_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


