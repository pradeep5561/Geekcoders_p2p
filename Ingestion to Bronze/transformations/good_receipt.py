# Databricks DLT pipeline: Ingest PDF invoices from a UC Volume into a Bronze Delta table
# This simple version just extracts full text from PDFs and saves in Bronze.

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
import io

from pypdf import PdfReader

# Config (set in DLT pipeline configuration)
catalog_name = spark.conf.get("catalog_name")
volume_path=f'/Volumes/{catalog_name}/staging/p2p_files/good_receipt'
primary_key='filename'

# UDF to extract text from PDF
@F.udf(returnType=T.StringType())
def pdf_bytes_to_text(pdf_bytes: bytes) -> str:
    if pdf_bytes is None:
        return None
    try:
        bio = io.BytesIO(pdf_bytes)
        reader = PdfReader(bio)
        pages_text = []
        for p in reader.pages:
            try:
                pages_text.append(p.extract_text() or "")
            except Exception:
                pages_text.append("")
        return "\n".join(pages_text).strip()
    except Exception:
        return None

# View to read PDFs as binary
@dlt.view(name="raw_gr_binary")
def raw_gr_binary() -> DataFrame:
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .load(volume_path)
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("filename", F.regexp_extract(F.col("path"), r"([^/]+)$", 1))
    )

dlt.create_streaming_table(
    name="good_receipt",
    comment="SCD1 Bronze target table"
)

dlt.apply_changes(
    target="good_receipt",
    source="raw_gr_binary",
    keys=[primary_key],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)
