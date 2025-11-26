# Databricks notebook source
# DBTITLE 1,Installlation of required libraries
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Adding widgets parameters
dbutils.widgets.text("vs_endpoint","p2p")
vs_endpoint = dbutils.widgets.get("vs_endpoint")
dbutils.widgets.text("catalog_name","dev_p2p")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# DBTITLE 1,Creation of vector search endpoint
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

# DBTITLE 1,creation of vector index

po_index_name = f"{catalog_name}.silver.purchase_order_index"
po_table = f"{catalog_name}.silver.purchase_order"  # Ensure change data feed is enabled on this table

# Create index if not exists
existing_indexes = [i["name"] for i in vs_client.list_indexes(vs_endpoint).get("indexes",{})]
print(existing_indexes)
try:
    if po_index_name not in existing_indexes:
        vs_client.create_delta_sync_index(
           endpoint_name=vs_endpoint,
           index_name=po_index_name,
           source_table_name=po_table,
           pipeline_type="TRIGGERED",
           primary_key="po_number",
           embedding_source_column="merged_description",
           embedding_model_endpoint_name="databricks-bge-large-en"
        )
        print(f"Index {po_index_name} created")
    else:
        print(f"Index {po_index_name} already exists")
except Exception as err:
    print("err",err)
    index = vs_client.get_index(index_name=po_index_name)
    index.sync()


# COMMAND ----------

# DBTITLE 1,Matching the po vs invoice data
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, BooleanType, IntegerType, DoubleType,DateType
import pandas as pd
from pyspark.sql.functions import *

result_schema = ArrayType(StructType([
    StructField("po_number", StringType()),
    StructField("vendor_name", StringType()),
    StructField("material_name", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("delivery_date", DateType()),
    StructField("score", FloatType())
]))
from datetime import datetime
@pandas_udf(result_schema)
def vector_search_results_udf(descriptions: pd.Series) -> pd.Series:
    client = VectorSearchClient()
    index = client.get_index(index_name=po_index_name)

    def get_results(text):
        try:
            results = index.similarity_search(
                query_text=text,
                columns=["po_number", "vendor_name", "material_name", "unit_price", "quantity","delivery_date"],
                num_results=1
            )
            formatted = []
            for r in results["result"]["data_array"]:
                # Adjust unpacking depending on index schema
              
                formatted.append({
                    "po_number": r[0],
                    "vendor_name": r[1],
                    "material_name": r[2],
                    "unit_price": float(r[3]),
                    "quantity": int(r[4]),
                    "delivery_date": datetime.strptime(r[5], "%Y-%m-%d").date() if r[5] else None,
                    "score": (r[6])
                })
            return formatted
        except Exception as e:
            print(f"Error during vector search for input: {str(text)[:30]}... → {e}")
            return []

    return descriptions.apply(get_results)

input_df = spark.read.table(f"{catalog_name}.silver.invoice")

# Call the UDF
df_results = input_df.withColumn(
    "vs_matches",
    vector_search_results_udf(col("merged_description"))
)


from delta.tables import DeltaTable

target_table = f"{catalog_name}.gold.invoice_vs_po_matches_temp"

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

# DBTITLE 1,Calculate the matched or unmatched data using llm model and
df_final = spark.sql(fr"""
SELECT
  invoice_number,
  po_number,
  vendor_name,
  material_name,
  invoice_qty,
  invoice_amount,
  parsed_result.match,
  parsed_result.reason
FROM (
  SELECT
    invoice_number,
    po_number,
    vendor_name,
    material_name,
    invoice_qty,
    invoice_amount,
    from_json(
      ai_query(
        "databricks-gpt-oss-120b",
        CONCAT(
          "You are an AI assistant that validates invoices against purchase orders. ",
          "Compare the invoice with the candidate matches and decide if it is a valid match. ",
          "Invoice details: ",
          "number = ", CAST(invoice_number AS STRING), ", ",
          "po = ", CAST(po_number AS STRING), ", ",
          "material = ", CAST(material_name AS STRING), ", ",
          "qty = ", CAST(invoice_qty AS STRING), ", ",
          "amount = ", CAST(invoice_amount AS STRING), ", ",
          "vendor = ", CAST(vendor_name AS STRING), ". ",
          "Candidate matches (JSON list): ", to_json(vs_matches), ". ",
          "Respond only in valid JSON with fields: match (true/false), reason (string)."
        )
      ),
      'STRUCT<match BOOLEAN, reason STRING>'
    ) AS parsed_result
  FROM {catalog_name}.gold.invoice_vs_po_matches_temp
)
""")

# COMMAND ----------

from delta.tables import DeltaTable

target_table = f"{catalog_name}.gold.2_way_match_results"

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

# COMMAND ----------

# MAGIC %skip
# MAGIC drop table dev_p2p.gold.2_way_match_results;
# MAGIC drop table dev_p2p.gold.invoice_vs_po_matches_temp;
# MAGIC drop table dev_p2p.silver.purchase_order_index;
