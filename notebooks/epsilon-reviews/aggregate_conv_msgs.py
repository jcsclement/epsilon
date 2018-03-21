# Databricks notebook source
# This will load data of messages and conversation and create a sparse dataframe and persist as CSV

# COMMAND ----------

# install python lib
!pip install pydocumentdb
# import libs
import pydocumentdb.documents as documents
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors
import datetime

# set up connectivity to Cosmos DB
uri = 'https://epsilon-cosmosdb.documents.azure.com:443/'
key = '3DehiQXbgrwTUtNUKUS1WKb55KZvN7FTzENU9zvnG9ze7XQB4jI9x3poZYK4ZI8AHHird6RxYEomqx7gH4IQQA=='
client = document_client.DocumentClient(uri, {'masterKey': key})

# Configure Database and Collections
databaseId = 'OC'
mcollectionId = 'Messages'
ccollectionId = 'Conversations'

# Configurations the Cosmos DB client will use to connect to the database and collection
dbLink = 'dbs/' + databaseId
mcollLink = dbLink + '/colls/' + mcollectionId
ccollLink = dbLink + '/colls/' + ccollectionId


# COMMAND ----------

# Fetching Messages
# form query
querystr = "select m.messageBody.cid as conversation_id, m.id as message_id, m.messageBody.text as message, m.channel as channel, m.messageBody.received as received, m.messageBody.authorId as author_id, m.messageBody.name as author_name from Messages as m order by m._ts"
# fire query
query = list(client.QueryDocuments(mcollLink, querystr, options=None, partition_key=None))
# Push into list `elements`
elements = list(query)
# Create `df` Spark DataFrame from `elements` Python list and converts to Pandas dataframe
mdf = spark.createDataFrame(elements)
pmdf = mdf.toPandas()

# COMMAND ----------

# Fetching Conversations
# form query
querystr = "select c.conversationId as conversation_id, c.type as type, c.sessionId as session_id, c.startTime as start_time, c.IsCompleted as is_completed, c.customerName as customer_name from Conversations as c order by c._ts"
# fire query
query = list(client.QueryDocuments(ccollLink, querystr, options=None, partition_key=None))
# Push into list `elements`
elements = list(query)
# Create `df` Spark DataFrame from `elements` Python list and converts to Pandas dataframe
cdf = spark.createDataFrame(elements)
display(cdf)
pcdf = cdf.toPandas()
#df.head()

# COMMAND ----------

display(mdf)

# COMMAND ----------

# register DataFrames as a temp tables so that we can query it using SQL
mdf.registerTempTable("temp_messages")
cdf.registerTempTable("temp_conversations")

# Perform the same query as the DataFrame above and return ``explain``
merged_sql = sqlContext.sql("SELECT m.message_id, m.conversation_id, c.session_id, m.message, m.author_name, c.customer_name, m.channel, c.start_time, c.is_completed, m.received FROM temp_messages m inner join temp_conversations c on m.conversation_id=c.conversation_id ")

# merged_sql.explain()
merged_sql.show(10)

# COMMAND ----------

# Converting temp table to Pandas dataframe
merged_pdf = merged_sql.toPandas()

# save dataframe as input csv file
# df = df[['conversation_id', 'type', 'session_id', 'start_time', 'is_completed', 'customer_name']]
# Fill na cells with backfill along every column
merged_pdf = merged_pdf.fillna(axis=0, method='backfill')
merged_pdf.to_csv("/dbfs/mnt/merged-convs/convdata.csv", sep=',', encoding='utf-8', index=False)

# COMMAND ----------

# MAGIC %fs ls "/mnt/merged-convs/"

# COMMAND ----------

