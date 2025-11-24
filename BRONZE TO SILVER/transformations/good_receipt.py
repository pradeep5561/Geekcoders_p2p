import dlt 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
catalog_name = spark.conf.get("catalog_name")
primary_key="gr_number"
@dlt.view(name='silver_gr')
def silver_gr():
    df=spark.readStream.table(f'{catalog_name}.bronze.good_receipt')
    df=df.selectExpr("ai_extract(text,array('gr_number','gr_date','po_number','material_name','received_qty','gr_status')) as data","filename","_load_timestamp")
    df=df.selectExpr("data.*","filename","_load_timestamp")
    df=df.withColumn("gr_date",F.to_date(F.col("gr_date"),"yyyy-MM-dd")).withColumn("received_qty",F.col("received_qty").cast("int")).withColumn("merged_description",F.concat_ws('|',F.col("po_number"),F.col("material_name")))
    return df

dlt.create_streaming_table(
    name="good_receipt",
    comment="SCD1 Silver target table",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="good_receipt",
    source="silver_gr",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)

