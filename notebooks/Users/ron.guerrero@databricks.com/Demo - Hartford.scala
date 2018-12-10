// Databricks notebook source
displayHTML("""<iframe src="https://www.thehartford.com/" width="100%" height="400"></iframe>""")

// COMMAND ----------

dbutils.widgets.text("inputDir", "box1","input directory")

// COMMAND ----------

dbutils.widgets.get("inputDir")

// COMMAND ----------

// MAGIC %md Run another notebook

// COMMAND ----------

// MAGIC %run 
// MAGIC ./data_Setup

// COMMAND ----------

// MAGIC %md lets peak into the data

// COMMAND ----------

// MAGIC %fs 
// MAGIC head wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json

// COMMAND ----------

// MAGIC %md 
// MAGIC Data Engineering in Scala

// COMMAND ----------

import org.apache.spark.sql._

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

case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

//fetch the JSON device information uploaded on our S3, now mounted on /mnt/datariders
val jsonFile = "wasbs://dataset@wesdias.blob.core.windows.net/Azure/dataset/horizontal/iot/devices.json"
//read the json file and create the dataset from the case class DeviceIoTData
// ds is now a collection of JVM Scala objects DeviccIoTData
val ds1 = spark.read.schema(jsonSchema).json(jsonFile).as[DeviceIoTData]

import org.apache.spark.sql.functions.upper
val ds = ds1.withColumn("Location", upper($"cn")).drop("cn")

// COMMAND ----------

//display Dataset's 
display(ds)

// COMMAND ----------

// MAGIC %md 
// MAGIC Data Engineering in Python

// COMMAND ----------

// MAGIC %python
// MAGIC dsIotDevices = spark.sql ("Select * from ronguerrero.rg_iot_devices")

// COMMAND ----------

// MAGIC %python 
// MAGIC import pyspark.sql.functions as f
// MAGIC dsTempDS = dsIotDevices.filter((f.col("temp") > 30) & (f.col("humidity") > 70))
// MAGIC display(dsTempDS)

// COMMAND ----------

// MAGIC %md 
// MAGIC Business User Analysis in SQL

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