import dlt 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
catalog_name = spark.conf.get("catalog_name")
primary_key="invoice_number"
@dlt.view(name='silver_invoice')
def silver_invoice():
    df=spark.readStream.table(f"{catalog_name}.bronze.invoice")
    df=df.selectExpr("ai_extract(text,array('invoice_number','invoice_date','po_number','material_name','invoice_qty','invoice_amount','vendor_name','due_date','payment_status')) as data","filename","_load_timestamp")
    df=df.selectExpr("data.*","filename","_load_timestamp")
    df=df.withColumn("invoice_date",F.to_date(F.col("invoice_date"),"yyyy-MM-dd")).withColumn("invoice_qty",F.col("invoice_qty").cast("int")).withColumn("invoice_amount",F.col("invoice_amount").cast("float")).withColumn("due_date",F.to_date(F.col("due_date"),"yyyy-MM-dd")).withColumn(
    "merged_description", F.concat_ws('|', F.col("po_number"), F.col("material_name"), F.col("vendor_name"))
)
    return df

dlt.create_streaming_table(
    name="invoice",
    comment="SCD1 Silver target table"
)

dlt.apply_changes(
    target="invoice",
    source="silver_invoice",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)

