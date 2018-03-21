// Databricks notebook source
// MAGIC %md # Connecting to Eventhub to consume incoming messages

// COMMAND ----------

import com.microsoft.azure._
import org.apache.spark.eventhubs._
import org.apache.spark.eventhubs.NameAndPartition
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import java.time.Instant


// Setup EventHubs configurations
val connectionString = "Endpoint=sb://epsilon-ingester.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=0avaFROCiITnOb1OekKiu2TUq56+YvvYgigSW6exPx0=;EntityPath=json-msgs-hub"

val ehConf = EventHubsConf(connectionString).setStartingPosition(EventPosition.fromEnqueuedTime(Instant.now))


// COMMAND ----------

// MAGIC %md # Loading stream of data in Spark dataframes to do further querying

// COMMAND ----------

// Loading the stream
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

// Use the business object that describes the dataset
case class Message(country_code: String, message_id: String, conversation_id: String, session_id: String, message: String, author_name: String,customer_name: String,channel: String,start_time: String,is_completed: Boolean,received: String)

import org.apache.spark.sql.Encoders
val schema = Encoders.product[Message].schema

val msgs = eventhubs.select(from_json(col("body").cast("string"), schema).alias("msg"))
msgs.printSchema

//Selecting child elements to move those up to root of dataframe
val data = msgs.select("msg.*")
data.printSchema

// COMMAND ----------

// MAGIC %md # Streaming to aggregate message counts in 1 minute window

// COMMAND ----------

// Time-Window based streaming
val streamingWindowCountsDF = 
  data
    .groupBy(window($"start_time", "1 minute"))
    .count()

streamingWindowCountsDF.isStreaming

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

val query =
  streamingWindowCountsDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("counts")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

// COMMAND ----------

// MAGIC 
// MAGIC %sql select date_format(window.end, "MMM-dd HH:mm") as win, count from counts order by win

// COMMAND ----------

// MAGIC %md # Streaming aggregation over message source location

// COMMAND ----------

// ========== DF with aggregation over location ==========
val countryDF = data.groupBy("country_code").count()

display(countryDF)

// COMMAND ----------

// ========== DF with aggregation over channels ==========
val aggDF = data.groupBy("channel").count()

display(aggDF)

// COMMAND ----------

// ========== DF with aggregation by customer_name ==========
val conversationAggDF = data.groupBy("customer_name").count()

display(conversationAggDF)

// COMMAND ----------

// MAGIC %md # Code Trials below

// COMMAND ----------

// ========== DF with aggregation ==========
val authorDF = data.groupBy("author_name").count()

// COMMAND ----------

display(authorDF)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
val query = aggDF.writeStream
  .outputMode("complete")
  .format("console")
  .option("checkpointLocation", "/tmp/chkpoint/")
  .trigger(Trigger.ProcessingTime("2 seconds"))
  .option("truncate", false)
  
query.start().awaitTermination()


// COMMAND ----------

import org.apache.spark.sql.DataFrame

def parseBody(df: DataFrame): DataFrame = {
  val temp = eventhubs.select(split(($"body"), ",").alias("msg"))
  df.withColumn("id", lit("hello world"))
}

// COMMAND ----------

var parsedDF = eventhubs.transform(parseBody)

// COMMAND ----------

val df = eventhubs.select(split(($"body"), ",").alias("msg"))

// COMMAND ----------

val query = df.writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false)
  
query.start().awaitTermination()

// COMMAND ----------


// Split the csv body into columns
//val words = eventhubs.as[String].flatMap(_.split(","))
//val df = eventhubs.select(split(($"body"), ",").alias("msg"))
//val df = eventhubs.selectExpr("CAST(body AS STRING)", "CAST(sequenceNumber AS LONG)")
//df.head()
val df = eventhubs.select("body").as[String]

// COMMAND ----------

df.isStreaming

// COMMAND ----------

df.printSchema

// COMMAND ----------

val csvdata = df.flatMap(_.split(","))


// COMMAND ----------

csvdata.explain()
csvdata.schema

// COMMAND ----------



// COMMAND ----------

var parsedDF = eventhubs.transform(parseBody)

// COMMAND ----------

val query = df.writeStream
  .outputMode("append")
  .format("console")
  .option("truncate", false)
  
query.start().awaitTermination()

// COMMAND ----------

//import com.microsoft.azure.eventhubs.impl._
import org.apache.spark.eventhubs.client._
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

val query =
  df
    .writeStream
    .format("console")        
    .trigger(Trigger.ProcessingTime(5.seconds))
    .start()

query.awaitTermination()

// COMMAND ----------

// Use the business object that describes the dataset
case class Message(id: String, convId: String, sId: String, message: String, author_name: String,customer_name: String,channel: String,start_time: String,is_completed: String,received: String)

import org.apache.spark.sql.Encoders
val schema = Encoders.product[Message].schema

val msgs = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .schema(schema)
  .load()
  //.as[Message]

//val mdf = msgs.count()

msgs.printSchema()
//msgs.count()
msgs.columns

val df2 = msgs.col("body").cast("string")

//df2.show()

// COMMAND ----------

msgs.isStreaming

// COMMAND ----------

val stats = msgs.groupBy('convId).agg(count('id) as "counts")

// COMMAND ----------

// Start the streaming query
// Write the result using console format, i.e. print to the console
// Only Complete output mode supported by groupBy
import scala.concurrent.duration._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
val msgStream = msgs.
  writeStream.
  format("console").
  trigger(Trigger.ProcessingTime(5.seconds)).
  //outputMode(OutputMode.Complete).
  //queryName("textStream").
  start()

// COMMAND ----------

msgStream.isActive

// COMMAND ----------

msgStream.stop