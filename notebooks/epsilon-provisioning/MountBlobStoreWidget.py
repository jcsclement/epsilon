# Databricks notebook source
#Accept params for this notebook
dbutils.widgets.text("StorageAccountName", "")
dbutils.widgets.text("StorageAccountAccessKey", "")
dbutils.widgets.text("StorageContainerName", "")
dbutils.widgets.text("StorageContainerSASKey", "")
dbutils.widgets.text("StorageFolderName", "")

# COMMAND ----------

#Create mount path
MountPointPath = "/mnt/" + dbutils.widgets.get("StorageFolderName")

# COMMAND ----------

str1 = "fs.azure.account.key." + dbutils.widgets.get("StorageAccountName") + ".blob.core.windows.net"
str2 = dbutils.widgets.get("StorageAccountAccessKey")
print(str1)
print(str2)
spark.conf.set(str1, str2)

str3 = "fs.azure.sas." + dbutils.widgets.get("StorageContainerName") + ".epsilonblobstore.blob.core.windows.net"
str4 = dbutils.widgets.get("StorageContainerSASKey")
print(str3)
print(str4)
spark.conf.set(str3, str4)

# COMMAND ----------

str5 = "wasbs://" + dbutils.widgets.get("StorageContainerName") + "@" + dbutils.widgets.get("StorageAccountName") + ".blob.core.windows.net/" + dbutils.widgets.get("StorageFolderName")
str6 = MountPointPath
str7 = "fs.azure.sas." + dbutils.widgets.get("StorageContainerName") + "." + dbutils.widgets.get("StorageAccountName") + ".blob.core.windows.net"
str8 = dbutils.widgets.get("StorageContainerSASKey")
print(str5)
print(str6)
print(str7)
print(str8)

# COMMAND ----------

#Mount storage
dbutils.fs.mount(
  source = str5, 
  mount_point = str6, 
  extra_configs = {str7:str8})

# COMMAND ----------

#Refresh mounts
dbutils.fs.refreshMounts()