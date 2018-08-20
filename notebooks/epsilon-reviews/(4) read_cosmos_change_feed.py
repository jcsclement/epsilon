# Databricks notebook source
# MAGIC %scala
# MAGIC import com.microsoft.azure.cosmosdb.spark.schema._
# MAGIC import com.microsoft.azure.cosmosdb.spark._
# MAGIC import com.microsoft.azure.cosmosdb.spark.config.Config
# MAGIC import org.codehaus.jackson.map.ObjectMapper
# MAGIC import com.microsoft.azure.cosmosdb.spark.streaming._
# MAGIC 
# MAGIC val configMap = Map("Endpoint" -> "https://epsilon-cosmosdb.documents.azure.com:443/", 
# MAGIC                     "Masterkey" -> "###",
# MAGIC                     "Database" -> "OC", 
# MAGIC                     "collection" -> "Messages", 
# MAGIC                     "ChangeFeedCheckpointLocation" -> "checkpointlocation", 
# MAGIC                     "changefeedqueryname" -> "Structured Stream interval count")
# MAGIC 
# MAGIC val sourceConfigMap = configMap.+(("changefeedqueryname", "Structured Stream replication streaming test"))
# MAGIC 
# MAGIC // Start reading change feed as a stream
# MAGIC var streamData = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(sourceConfigMap).load()
# MAGIC 
# MAGIC // Start streaming query to console sink
# MAGIC val query = streamData.withColumn("countcol", streamData.col("id").substr(0, 0)).groupBy("countcol").count().writeStream.outputMode("complete").format("console").start()

# COMMAND ----------

