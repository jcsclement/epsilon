// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.epsilonblobstore.blob.core.windows.net",
  "AE8bTgw6gMzktzeec6n+CtVD1j1Hzcq+gfQLqH+2LLpdRhfHxHRe03Q8zHxXxVHYZ+/OT+TJ8M30/NiPyCVBzA==")
spark.conf.set(
  "fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net",
  "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D")

// COMMAND ----------

dbutils.fs.unmount("/mnt/mount-test01")
dbutils.fs.unmount("/mnt/mount-test02")
dbutils.fs.unmount("/mnt/mount-test03")
dbutils.fs.unmount("/mnt/mount-test10")
dbutils.fs.unmount("/mnt/mount-test12")
dbutils.fs.unmount("/mnt/mount-test13")
dbutils.fs.unmount("/mnt/mount-test14")
dbutils.fs.unmount("/mnt/pq-convs")
dbutils.fs.unmount("/mnt/media-breakdowns-new")
dbutils.fs.unmount("/mnt/media-breakdowns-processed")
dbutils.fs.unmount("/mnt/media-breakdowns")
dbutils.fs.unmount("/mnt/epsilon-dir")
dbutils.fs.unmount("/mnt/merged-convs")
dbutils.fs.unmount("/mnt/messages")
dbutils.fs.unmount("/mnt/conversations")
dbutils.fs.unmount("/mnt/cosmosdb-jars")
dbutils.fs.unmount("/mnt/epsilon-reviews")

// COMMAND ----------

// MAGIC %fs refreshMounts

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

