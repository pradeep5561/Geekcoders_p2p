import dlt 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
catalog_name = spark.conf.get("catalog_name")
primary_key="po_number"
@dlt.view(name='silver_po')
def silver_gr():
    df=spark.readStream.table(f'{catalog_name}.bronze.purchase_order')
    df=df.selectExpr("ai_extract(text,array('po_number','po_date','vendor_name','material_name','material_category','material_unit','quantity','unit_price','delivery_date')) as data","filename","_load_timestamp")
    df=df.selectExpr("data.*","filename","_load_timestamp")
    df=df.withColumn("po_date",F.to_date(F.col("po_date"),"yyyy-MM-dd")).withColumn("quantity",F.col("quantity").cast("int")).withColumn("unit_price",F.col("unit_price").cast("int")).withColumn("delivery_date",F.to_date(F.col("delivery_date"),"yyyy-MM-dd")).withColumn("merged_description",F.concat_ws('|',F.col("po_number"),F.col("vendor_name"),F.col("material_name")))
    return df

dlt.create_streaming_table(
    name="purchase_order",
    comment="SCD1 Silver target table",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)

dlt.apply_changes(
    target="purchase_order",
    source="silver_po",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)

