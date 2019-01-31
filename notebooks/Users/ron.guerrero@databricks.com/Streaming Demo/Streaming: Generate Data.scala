// Databricks notebook source
// MAGIC %md
// MAGIC # Data Generation
// MAGIC 
// MAGIC ## You must run this first!!!!!
// MAGIC This notebook generates random data into a delta table.    
// MAGIC 
// MAGIC The current schema is as follows:  
// MAGIC ```
// MAGIC ts : timestamp,  some_col : string,  code : int
// MAGIC ```
// MAGIC 
// MAGIC ts: timestamp of the record   
// MAGIC some_col: will be an randomly generated string   
// MAGIC code: random code number between 0..5   
// MAGIC 
// MAGIC NOTE: The delta table's underlying directory will be deleted and recreated. As well, the metastore definitions will be dropped and created.

// COMMAND ----------

// MAGIC %md 
// MAGIC Clean up the delta directories, and start new

// COMMAND ----------

// MAGIC %fs
// MAGIC rm /ronguerrero/delta/stream_demo

// COMMAND ----------

// MAGIC %fs
// MAGIC mkdirs /ronguerrero/delta/stream_demo

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create the metastore definitions so we can reference as delta tables

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS ronguerrero.input_stream;
// MAGIC CREATE TABLE IF NOT EXISTS ronguerrero.input_stream (ts timestamp, some_col string, code int) USING DELTA LOCATION '/ronguerrero/delta/input_stream';

// COMMAND ----------

import org.apache.spark.sql._

import org.apache.spark.sql.types._
import java.sql.Timestamp
import scala.util.Random

case class Record(ts: Timestamp, some_col: String, code: Int)

def genRecords : DataFrame = {
val numRecs = 1
var records = Seq[Record]()
   for( x <- 1 to numRecs ) {
      records = records :+ new Record(new Timestamp( System.currentTimeMillis() ), s"name_${Random.alphanumeric take 10 mkString}", Random.nextInt(5))
   }
  records.toDF
}

while (true) {
  val df = genRecords
 df.write
  .format("delta")
  .mode("append")
  .save("/ronguerrero/delta/input_stream")
}