import dlt
from pyspark.sql.functions import current_timestamp

# Config
catalog_name = spark.conf.get("catalog_name")
volume_path = f"/Volumes/{catalog_name}/staging/p2p_files/material"
primary_key = "material_id"
# Use a unique checkpoint path

@dlt.view(
    name="bronze_raw_material"
)
def bronze_raw_stream():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(volume_path)
        .withColumn("_load_timestamp", current_timestamp())
    )

dlt.create_streaming_table(
    name="material",
    comment="SCD1 Bronze target table"
)

dlt.apply_changes(
    target="material",
    source="bronze_raw_material",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)