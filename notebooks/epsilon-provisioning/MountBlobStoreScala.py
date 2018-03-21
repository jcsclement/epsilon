# Databricks notebook source
# MAGIC %scala
# MAGIC var StorageAccountName = "epsilonblobstore"
# MAGIC var StorageAccountAccessKey = "AE8bTgw6gMzktzeec6n+CtVD1j1Hzcq+gfQLqH+2LLpdRhfHxHRe03Q8zHxXxVHYZ+/OT+TJ8M30/NiPyCVBzA=="
# MAGIC var StorageContainerName = "epsilon-container"
# MAGIC var StorageContainerSASKey = "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"
# MAGIC var StorageFolderName = "mount-test02"
# MAGIC var MountPointPath = "/mnt/" + StorageFolderName

# COMMAND ----------

# MAGIC %scala
# MAGIC var str1 = "fs.azure.account.key." + StorageAccountName + ".blob.core.windows.net"
# MAGIC var str2 = StorageAccountAccessKey
# MAGIC spark.conf.set(str1, str2)
# MAGIC 
# MAGIC var str3 = "fs.azure.sas." + StorageContainerName + ".epsilonblobstore.blob.core.windows.net"
# MAGIC var str4 = StorageContainerSASKey
# MAGIC spark.conf.set(str3, str4)

# COMMAND ----------

# MAGIC %scala
# MAGIC var str5 = "wasbs://" + StorageContainerName + "@" + StorageAccountName + ".blob.core.windows.net/" + StorageFolderName
# MAGIC var str6 = MountPointPath
# MAGIC var str7 = "fs.azure.sas." + StorageContainerName + "." + StorageAccountName + ".blob.core.windows.net"
# MAGIC var str8 = StorageContainerSASKey

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC   source = str5, 
# MAGIC   mountPoint = str6, 
# MAGIC   extraConfigs = Map(str7 -> str8))