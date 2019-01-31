// Databricks notebook source
// MAGIC %md
// MAGIC # Data Generation
// MAGIC ## You must run Streaming: Generate Data first!!!!!
// MAGIC This notebook will stream data from a delta table a perform transformation on the fly via a UDF

// COMMAND ----------

import org.apache.spark.sql.functions._

// define the UDF to convert numeric code to a string
val convertCode = udf { s: Int => 
val lookup = Map(1 -> "One", 2 -> "Two", 3 -> "Three", 4 -> "Four", 5 -> "Five")
  lookup.get(s) getOrElse "Unknown"
}

// stream the data from the delta table
val stream = spark.readStream
  .format("delta")
  .table("ronguerrero.input_stream")
  .withColumn("string_code", convertCode(col("code")))
  .where(( unix_timestamp(current_timestamp) - unix_timestamp(col("ts"))) < 2)

// lets display it
display(stream)

// COMMAND ----------

