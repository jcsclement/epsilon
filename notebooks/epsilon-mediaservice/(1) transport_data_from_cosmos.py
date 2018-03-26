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
key = '<Cosmos Key>'
client = document_client.DocumentClient(uri, {'masterKey': key})

# COMMAND ----------

# Configure Database and Collections
databaseId = 'OC'
mcollectionId = 'Messages'
ccollectionId = 'Conversations'
bdcollectionId = 'ConversationBreakdown'

# Configurations the Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
mcollLink = dbLink + '/colls/' + mcollectionId
ccollLink = dbLink + '/colls/' + ccollectionId
bdcollLink = dbLink + '/colls/' + bdcollectionId

# COMMAND ----------

# form query
querystr = "select bd.id as activityId, bd.breakdown.summarizedInsights.topics[0].name as topic1, bd.breakdown.summarizedInsights.topics[1].name as topic2, bd.breakdown.summarizedInsights.topics[2].name as topic3 from ConversationBreakdown as bd"
# fire query
query = list(client.QueryDocuments(bdcollLink, querystr, options=None, partition_key=None))
# Push into list `elements`
elements = list(query)
# Create `df` Spark DataFrame from `elements` Python list and converts to Pandas dataframe
df_topics = spark.createDataFrame(elements).toPandas()
df_topics.head()

# COMMAND ----------

# save dataframe as input csv file
df_topics = df_topics[['activityId', 'topic1', 'topic2', 'topic3']]
# Fill na cells with backfill along every column
df_topics = df_topics.fillna(axis=0, method='backfill')
df_topics.to_csv("/dbfs/mnt/epsilon-reviews/mediatopics.csv", sep=',', encoding='utf-8', index=False)

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/'

# COMMAND ----------

df_topics

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

