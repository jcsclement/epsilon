// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.epsilonblobstore.blob.core.windows.net",
  "<Storage Account Access Key>")
spark.conf.set(
  "fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net",
  "<SAS Key for folder>")

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

