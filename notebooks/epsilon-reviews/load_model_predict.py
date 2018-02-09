# Databricks notebook source
import sys
import pandas as pd
from sklearn import metrics
import numpy as np

# COMMAND ----------

df_in = pd.read_csv('/dbfs/mnt/epsilon-reviews/input.csv')
#dataset_in = spark_df.toPandas()
dataset_in = df_in.iloc[:, 2].tolist()
print("n_samples: %d" % len(dataset_in))

# COMMAND ----------

# load model
from sklearn.externals import joblib
grid_search = joblib.load('/dbfs/mnt/epsilon-reviews/predictSentiment.pkl')
# predict
dataset_out = grid_search.predict(dataset_in)

# COMMAND ----------

# convert both input and output to numpy arrays
array_out = np.asarray(dataset_out)
array_in = df_in.as_matrix()

# COMMAND ----------

# join arrays to get a dataframe
out_data = np.column_stack((array_in, array_out))

# COMMAND ----------

# save dataframe as a csv file
df_out_data = pd.DataFrame(data=out_data, columns=['conversation_id', 'message_id', 'message', 'sentiment'])
df_out_data.to_csv("/dbfs/mnt/epsilon-reviews/output.csv", sep=',', encoding='utf-8', index=False)