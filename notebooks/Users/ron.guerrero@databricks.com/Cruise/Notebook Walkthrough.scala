// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC <div style="text-align: left; line-height: 0; padding-top: 9px;">
// MAGIC   <p style = "font-size:30px">
// MAGIC   <img src="https://getcruise.com/images/orange-logo.png" height="100" width="250"> <b></b>
// MAGIC   </p>
// MAGIC </div>

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Demo Highlights
// MAGIC 
// MAGIC IOT data from sensors.   
// MAGIC Sensor Metrics
// MAGIC * location
// MAGIC * temperature
// MAGIC * humidity
// MAGIC 
// MAGIC ### Step 1: Data Engineering
// MAGIC ### Step 2: Data Analytics (ML)
// MAGIC ### Step 3: Collaboration Capabilities 

// COMMAND ----------

// MAGIC %md
// MAGIC # Step 1: Data Engineering

// COMMAND ----------

// MAGIC %run 
// MAGIC ./data_Setup

// COMMAND ----------

// MAGIC %fs 
// MAGIC ls wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/

// COMMAND ----------

// MAGIC %fs 
// MAGIC head wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json

// COMMAND ----------

// MAGIC %md Let's create a schema so Spark doesn't have to infer it and reading it is a lot faster

// COMMAND ----------

import org.apache.spark.sql.types._

val jsonSchema = new StructType()
        .add("battery_level", LongType)
        .add("c02_level", LongType)
        .add("cca2", StringType)
        .add("cca3",StringType)
        .add("cn", StringType)
        .add("device_id", LongType)
        .add("device_name", StringType)
        .add("humidity", LongType)
        .add("ip", StringType)
        .add("latitude", DoubleType)
        .add("lcd", StringType)
        .add("longitude", DoubleType)
        .add("scale", StringType)
        .add("temp", LongType)
        .add("timestamp", TimestampType)

// COMMAND ----------

// MAGIC %md Create a Scala case class to represent your IoT Device Data

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

// COMMAND ----------

import org.apache.spark.sql._

//fetch the JSON device information uploaded on our S3, now mounted on /mnt/datariders
val jsonFile = "wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json"
//read the json file and create the dataset from the case class DeviceIoTData
// ds is now a collection of JVM Scala objects DeviccIoTData
val ds1 = spark.read.schema(jsonSchema).json(jsonFile).as[DeviceIoTData]

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Data Transformation

// COMMAND ----------

import org.apache.spark.sql.functions.upper
val ds = ds1.withColumn("Location", upper($"cn")).drop("cn")

// COMMAND ----------

//ds.write.saveAsTable("ronguerrero.rg_iot_devices")

// COMMAND ----------

//display Dataset's 
display(ds)

// COMMAND ----------

// MAGIC %md 
// MAGIC #Step 2: Data Analysis 

// COMMAND ----------

// MAGIC %md Let's iterate over the first 10 entries with the foreach() method and print them. Notice this is quite similar to RDD but a lot easier.
// MAGIC 
// MAGIC Do keep this tab open in your browser. [Dataset API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

// COMMAND ----------

// MAGIC %md ####Find out all devices with temperatures exceeding 30 and humidity greater than 70

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dsIotDevices = spark.sql ("Select * from ronguerrero.rg_iot_devices")

// COMMAND ----------

// MAGIC %python 
// MAGIC import pyspark.sql.functions as f
// MAGIC dsTempDS = dsIotDevices.filter((f.col("temp") > 30) & (f.col("humidity") > 70))
// MAGIC display(dsTempDS)

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Optional ML

// COMMAND ----------

// MAGIC %md 
// MAGIC #Step 3: Visualizing datasets using various display methods and graphs

// COMMAND ----------

// MAGIC %md #### Count all devices for a partiular country and display them using Spark SQL
// MAGIC Note: Limit to 100 rows

// COMMAND ----------

// MAGIC %sql 
// MAGIC select cca3, count(distinct device_id) as device_id from ronguerrero.rg_iot_devices group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md Let's visualize the results as a pie chart and distribution for devices in the country where C02 are high.

// COMMAND ----------

// MAGIC %sql 
// MAGIC select cca3, c02_level from rg_iot_devices where c02_level > 1400 order by c02_level desc

// COMMAND ----------

// MAGIC %md Select all countries' devices with high-levels of C02 and group by cca3 and order by device_ids 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select cca3, count(distinct device_id) as device_id from rg_iot_devices where lcd == 'red' group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md Find out all devices in countries whose batteries need replacements 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select cca3, count(distinct device_id) as device_id from rg_iot_devices where battery_level == 0 group by cca3 order by device_id desc limit 100

// COMMAND ----------

// MAGIC %md 
// MAGIC #Step 4: Streaming

// COMMAND ----------

// MAGIC %md Parquet is an efficient columnar file format to save transformed data for columnar analysis. For example, after your initial IoT ETL, you want to save
// MAGIC for other applications down the pipeline to do further analysis, in the data format you have cleaned, Parquet is the recommended file format.

// COMMAND ----------

for (x <- 1 to 10) {
ds.write.mode("append").json("/tmp/ronguerrero-iotDevicesRaw")
}


// COMMAND ----------

val streamDF = spark
  .readStream
  .schema(jsonSchema)
  .option("maxFilesPerTrigger", 1)
  .json("/tmp/ronguerrero-iotDevicesRaw")

streamDF
  .writeStream
  .format("delta")
  .option("path", "/tmp/ronguerrero-iotDevicesParquet1/")
  .option("checkpointLocation", "/tmp/ronguerrero-iotDevicesParquet1/checkpoint/")
  .start()

// COMMAND ----------

display(
  spark
  .readStream
  .format("delta")
  .load("/tmp/ronguerrero-iotDevicesParquet1")
  .filter("temp > '27'")
  .groupBy("cca3")
  .count
  .orderBy($"count".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Summary
// MAGIC 
// MAGIC ### 1) Single Platform for both Big Data Processing and Advanced Analytics (ML)
// MAGIC * The ability for Data Engineering and Advanced analytics within the same environment
// MAGIC * Tool Flexibility
// MAGIC * Streaming 
// MAGIC 
// MAGIC ### 2) Collaborative environment  
// MAGIC * Feature are all built-in
// MAGIC * Ability to concurrently work in the same notebook
// MAGIC * Comments, Revision History, GitHub Integration
// MAGIC 
// MAGIC  

// COMMAND ----------

