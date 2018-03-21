# Databricks notebook source
StorageAccountName = "epsilonblobstore"
StorageAccountAccessKey = "AE8bTgw6gMzktzeec6n+CtVD1j1Hzcq+gfQLqH+2LLpdRhfHxHRe03Q8zHxXxVHYZ+/OT+TJ8M30/NiPyCVBzA=="
StorageContainerName = "epsilon-container"
StorageContainerSASKey = "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"
StorageFolderName = "mount-test03"
MountPointPath = "/mnt/" + StorageFolderName

# COMMAND ----------

str1 = "fs.azure.account.key." + StorageAccountName + ".blob.core.windows.net"
str2 = StorageAccountAccessKey
spark.conf.set(str1, str2)

str3 = "fs.azure.sas." + StorageContainerName + ".epsilonblobstore.blob.core.windows.net"
str4 = StorageContainerSASKey
spark.conf.set(str3, str4)

# COMMAND ----------

str5 = "wasbs://" + StorageContainerName + "@" + StorageAccountName + ".blob.core.windows.net/" + StorageFolderName
str6 = MountPointPath
str7 = "fs.azure.sas." + StorageContainerName + "." + StorageAccountName + ".blob.core.windows.net"
str8 = StorageContainerSASKey

# COMMAND ----------

dbutils.fs.mount(
  source = str5, 
  mount_point = str6, 
  extra_configs = {str7:str8})