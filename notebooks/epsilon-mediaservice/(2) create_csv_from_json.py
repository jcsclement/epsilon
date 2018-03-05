# Databricks notebook source
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import explode
import os
import pandas as pd
sqlContext = SQLContext(sc)
# Read all jsons in folder /mnt/media-breakdowns
breakDown = sqlContext.read.json("/mnt/media-breakdowns/*.json")
# Register as Temptable
breakDown.registerTempTable("breakDown")
SDF = sqlContext.sql("SELECT breakdowns[0].id as breakdownId, breakdowns[0].insights.topics as topics, breakdowns[0].insights.brands as brands FROM breakDown")

# COMMAND ----------

# explode topics
topicsSDF3 = SDF.select('breakdownId', 'topics'). \
              selectExpr("breakdownId as breakdownId", "explode(topics) AS topicsCol"). \
              selectExpr("breakdownId as breakdownId", "topicsCol.name as topic", "topicsCol.rank as rank")
#topicsSDF3.show()
# Write into csv from SQL dataframe
#topicsSDF3.write.format('com.databricks.spark.csv').mode('overwrite').save('/mnt/media-breakdowns-processed/topics.csv')
# Load from CSV
#RDF = spark.read.format('csv').load('/mnt/media-breakdowns-processed/topics.csv')
#RDF.show()
# Convert to pandas dataframe
PDF = topicsSDF3.toPandas()
# write csv from pandas dataframe
# if file does not exist write header 
if not os.path.isfile('/dbfs/mnt/media-breakdowns-processed/topics.csv'):
  PDF.to_csv(os.path.join('/dbfs/mnt/media-breakdowns-processed', 'topics.csv'), index = False)  
else: # if file exists, then do not write header but append records
  PDF.to_csv(os.path.join('/dbfs/mnt/media-breakdowns-processed', 'topics.csv'), index = False, header = False, mode = 'a')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

