import dlt 
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
catalog_name = spark.conf.get("catalog_name")
primary_key="vendor_id"
@dlt.view(name='silver_vendor')
def silver_vendor():
    df=spark.readStream.table(f'{catalog_name}.bronze.vendor').drop('_rescued_data')
    return df

dlt.create_streaming_table(
    name="vendor",
    comment="SCD1 Silver target table"
)

dlt.apply_changes(
    target="vendor",
    source="silver_vendor",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)

