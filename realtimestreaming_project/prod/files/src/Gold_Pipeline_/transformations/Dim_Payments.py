from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {
    "rule 1" : "payment_method_id IS NOT NULL",
    "rule 2" : "payment_method IS NOT NULL"
}
@dp.view(
    name = "Dim_Payment_Method_stg"
)
def Dim_Payment_Method_stg():
    df_payments = spark.readStream.table("realtime_project.silver.bulk_rides_obt")
    df_payments = df_payments.select(
  "payment_method_id", "payment_method", "is_card", "requires_auth", "is_completed"
    ).dropDuplicates(["payment_method_id"])\
     .withColumn("last_updated_timestamp", current_timestamp())
    return df_payments

dp.create_streaming_table(name= "Dim_Payment_Methods", expect_all_or_fail=(expectations), table_properties= {
    "pipelines.autoOptimize.zOrderCols": "payment_method_id"
})

dp.create_auto_cdc_flow(
    target = "Dim_Payment_Methods",
    source = "Dim_Payment_Method_stg",
    keys = ["payment_method_id"],
  sequence_by = "last_updated_timestamp",
  stored_as_scd_type = "2",
  track_history_except_column_list = None,
  except_column_list = ["last_updated_timestamp"],
  name = None,
  once = False
)


     
