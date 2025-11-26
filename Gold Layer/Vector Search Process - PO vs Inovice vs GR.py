# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("vs_endpoint","p2p")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
dbutils.widgets.text("catalog_name","dev_p2p")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk.errors.platform import ResourceAlreadyExists
vs_client = VectorSearchClient(disable_notice=True)
try:
    # Check if endpoint already exists
    endpoints = vs_client.list_endpoints().get("endpoints", [])
    existing = any(ep["name"] == vs_endpoint for ep in endpoints)

    if existing:
        print(f"✅ Endpoint '{vs_endpoint}' already exists. Skipping creation.")
    else:
        vs_client.create_endpoint(
            name=vs_endpoint,
            endpoint_type="STANDARD"   # or "STORAGE_OPTIMIZED"
        )
        print(f"🚀 Endpoint '{vs_endpoint}' created successfully.")

except ResourceAlreadyExists:
    print(f"⚠️ Endpoint '{vs_endpoint}' already exists (caught by SDK).")
except Exception as e:
    print(f"❌ Failed to create or check endpoint: {e}")

# COMMAND ----------

po_index_name = f"{catalog_name}.silver.purchase_order_index"
po_table = f"{catalog_name}.silver.purchase_order"
gr_index_name = f"{catalog_name}.silver.good_receipt_index"
gr_table = f"{catalog_name}.silver.good_receipt"

# Create PO index if not exists
existing_indexes = [i["name"] for i in vs_client.list_indexes(vs_endpoint).get("indexes",{})]


# Create GR Index
try:
    if gr_index_name not in existing_indexes:
        vs_client.create_delta_sync_index(
           endpoint_name=vs_endpoint,
           index_name=gr_index_name,
           source_table_name=gr_table,
           pipeline_type="TRIGGERED",
           primary_key="gr_number",
           embedding_source_column="merged_description",  # Should be po_number|material_name|vendor_name
           embedding_model_endpoint_name="databricks-bge-large-en"
        )
        print(f"Index {gr_index_name} created")
    else:
        print(f"Index {gr_index_name} already exists")
except Exception as err:
    print("GR Index error:", err)
    index = vs_client.get_index(index_name=gr_index_name)
    index.sync()



# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, BooleanType, IntegerType, DoubleType,DateType
import pandas as pd
from pyspark.sql.functions import *
from datetime import datetime

po_result_schema = ArrayType(StructType([
    StructField("po_number", StringType()),
    StructField("vendor_name", StringType()),
    StructField("material_name", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("delivery_date", DateType()),
    StructField("score", FloatType())
]))

# GR search result schema
gr_result_schema = ArrayType(StructType([
    StructField("gr_number", StringType()),
    StructField("po_number", StringType()),
    StructField("gr_date", DateType()),
    StructField("material_name", StringType()),
    StructField("received_qty", IntegerType()),
    StructField("gr_status", StringType()),
    StructField("score", FloatType())
]))

@pandas_udf(po_result_schema)
def po_vector_search_udf(descriptions: pd.Series) -> pd.Series:
    client = VectorSearchClient()
    index = client.get_index(index_name=po_index_name)

    def get_po_results(text):
        try:
            results = index.similarity_search(
                query_text=text,
                columns=["po_number", "vendor_name", "material_name", "unit_price", "quantity","delivery_date"],
                num_results=1  # Get top 3 candidates for better matching
            )
            formatted = []
            for r in results["result"]["data_array"]:
                formatted.append({
                    "po_number": r[0],
                    "vendor_name": r[1],
                    "material_name": r[2],
                    "unit_price": float(r[3]) if r[3] else 0.0,
                    "quantity": int(r[4]) if r[4] else 0,
                    "delivery_date": datetime.strptime(r[5], "%Y-%m-%d").date() if r[5] else None,
                    "score": float(r[6]) if r[6] else 0.0
                })
            return formatted
        except Exception as e:
            print(f"Error during PO vector search for input: {str(text)[:30]}... → {e}")
            return []

    return descriptions.apply(get_po_results)

@pandas_udf(gr_result_schema)
def gr_vector_search_udf(descriptions: pd.Series) -> pd.Series:
    client = VectorSearchClient()
    index = client.get_index(index_name=gr_index_name)

    def get_gr_results(text):
        try:
            results = index.similarity_search(
                query_text=text,
                columns=["gr_number", "po_number", "gr_date", "material_name", "received_qty", "gr_status"],
                num_results=1 # Get top 3 candidates
            )
            formatted = []
            for r in results["result"]["data_array"]:
                formatted.append({
                    "gr_number": r[0],
                    "po_number": r[1],
                    "gr_date": datetime.strptime(r[2], "%Y-%m-%d").date() if r[2] else None,
                    "material_name": r[3],
                    "received_qty": int(r[4]) if r[4] else 0,
                    "gr_status": r[5],
                    "score": float(r[6]) if r[6] else 0.0
                })
            return formatted
        except Exception as e:
            print(f"Error during GR vector search for input: {str(text)[:30]}... → {e}")
            return []

    return descriptions.apply(get_gr_results)

# Read invoice data and prepare merged description
input_df = spark.read.table(f"{catalog_name}.silver.invoice")
# Execute vector search against both PO and GR indexes
df_results = input_df \
    .withColumn("po_matches", po_vector_search_udf(col("merged_description"))) \
    .withColumn("gr_matches", gr_vector_search_udf(col("merged_description")))

# Save intermediate results

from delta.tables import DeltaTable

target_table = f"{catalog_name}.gold.invoice_vs_po_gr_matches_temp"

if spark.catalog.tableExists(target_table):
    # Table exists → do MERGE
    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(
            df_results.alias("s"),
            "t.invoice_number = s.invoice_number"  # 👈 merge key, adjust as needed
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE completed into {target_table}")
else:
    # Table does not exist → create new table
    df_results.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"🆕 Created new table {target_table}")


# COMMAND ----------

df_final = spark.sql(fr"""
SELECT
  invoice_number,
  po_number,
  invoice_date,
  vendor_name,
  material_name,
  invoice_qty,
  invoice_amount,
  parsed_result.overall_match,
  parsed_result.po_match,
  parsed_result.gr_match,
  parsed_result.reason,
  parsed_result.confidence,
  parsed_result.discrepancies,
  parsed_result.matched_po_number,
  parsed_result.matched_gr_number
FROM (
  SELECT
    invoice_number,
    invoice_date,
    po_number,
    vendor_name,
    material_name,
    invoice_qty,
    invoice_amount,
    from_json(
      ai_query(
        "databricks-gpt-oss-120b",
        CONCAT(
          "You are an AI assistant performing 3-way document matching between Invoice, Purchase Order (PO), and Goods Receipt (GR). ",
          "Your task is to validate if the invoice is legitimate by checking against both PO and GR records. ",
          "\n\nINVOICE DETAILS:\n",
          "Invoice Number: ", CAST(invoice_number AS STRING), "\n",
          "invoice_date:", CAST(invoice_date AS STRING), "\n", 
          "PO Number: ", CAST(po_number AS STRING), "\n",
          "Material: ", CAST(material_name AS STRING), "\n",
          "Vendor: ", CAST(vendor_name AS STRING), "\n",
          "Invoice Quantity: ", CAST(invoice_qty AS STRING), "\n",
          "Invoice Amount: ", CAST(invoice_amount AS STRING), "\n",
          "\n\nPO CANDIDATE MATCHES:\n", to_json(po_matches), 
          "\n\nGR CANDIDATE MATCHES:\n", to_json(gr_matches),
          "\n\nVALIDATION RULES:\n",
          "1. Find the best matching PO based on po_number, material_name, and vendor_name\n",
          "2. Find the best matching GR that references the same PO\n",
          "3. Invoice quantity should not exceed GR received quantity\n",
          "4. Invoice amount should align with PO unit_price * invoice_qty (allow 5% tolerance)\n",
          "5. All three documents should have matching vendor and material\n",
          "6. GR receipt_date should be before or close to invoice date\n",
          "7. GR receipt_date should be before or close to invoice date if this happens mark this gr_match as false\n",
          "\nRESPONSE FORMAT (JSON only):\n",
          "{{\n",
          "  \"overall_match\": boolean,\n",
          "  \"po_match\": boolean,\n",
          "  \"gr_match\": boolean,\n",
          "  \"reason\": \"string\",\n",
          "  \"confidence\": float (0.0 to 1.0),\n",
          "  \"discrepancies\": [\"list of issues found\"],\n",
          "  \"matched_po_number\": \"best matching PO number or null\",\n",
          "  \"matched_gr_number\": \"best matching GR number or null\"\n",
          "}}"
        )
      ),
      'STRUCT<overall_match BOOLEAN, po_match BOOLEAN, gr_match BOOLEAN, reason STRING, confidence DOUBLE, discrepancies ARRAY<STRING>, matched_po_number STRING, matched_gr_number STRING>'
    ) AS parsed_result
  FROM {catalog_name}.gold.invoice_vs_po_gr_matches_temp
)
""")



from delta.tables import DeltaTable

target_table = f"{catalog_name}.gold.three_way_match_results"

if spark.catalog.tableExists(target_table):
    # Table exists → do MERGE
    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(
            df_final.alias("s"),
            "t.invoice_number = s.invoice_number"  # 👈 merge key, adjust as needed
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE completed into {target_table}")
else:
    # Table does not exist → create new table
    df_final.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"🆕 Created new table {target_table}")
