import dlt 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
catalog_name = spark.conf.get("catalog_name")
primary_key="material_id"
@dlt.view(name='silver_material')
def silver_material():
    df=spark.readStream.table(f'{catalog_name}.bronze.material').drop('_rescued_data')
    df=df.withColumn("price_estimate",F.col("price_estimate").cast('int'))

    return df

dlt.create_streaming_table(
    name="material",
    comment="SCD1 Silver target table"
)

dlt.apply_changes(
    target="material",
    source="silver_material",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)

