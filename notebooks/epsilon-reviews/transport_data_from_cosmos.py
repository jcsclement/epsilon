# Databricks notebook source
# install python lib
!pip install pydocumentdb

# COMMAND ----------

# import libs
import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors
import datetime

# COMMAND ----------

# set up connectivity to Cosmos DB
uri = 'https://epsilon-cosmosdb.documents.azure.com:443/'
key = '3DehiQXbgrwTUtNUKUS1WKb55KZvN7FTzENU9zvnG9ze7XQB4jI9x3poZYK4ZI8AHHird6RxYEomqx7gH4IQQA=='
client = document_client.DocumentClient(uri, {'masterKey': key})

# COMMAND ----------

# Configure Database and Collections
databaseId = 'OC'
collectionId = 'Messages'

# Configurations the Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
collLink = dbLink + '/colls/' + collectionId

# COMMAND ----------

# form query
querystr = "select top 100 m.messageBody.cid as conversation_id, m.id as message_id, m.messageBody.text as message from Messages as m order by m._ts desc"
# fire query
query = list(client.QueryDocuments(collLink, querystr, options=None, partition_key=None))
# Push into list `elements`
elements = list(query)
# Create `df` Spark DataFrame from `elements` Python list and converts to Pandas dataframe
df = spark.createDataFrame(elements).toPandas()

# COMMAND ----------

# save dataframe as input csv file
df = df[['conversation_id', 'message_id', 'message']]
# Fill na cells with backfill along every column
df = df.fillna(axis=0, method='backfill')
df.to_csv("/dbfs/mnt/epsilon-reviews/input.csv", sep=',', encoding='utf-8', index=False)