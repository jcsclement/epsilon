// Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.epsilonblobstore.blob.core.windows.net",
  "AE8bTgw6gMzktzeec6n+CtVD1j1Hzcq+gfQLqH+2LLpdRhfHxHRe03Q8zHxXxVHYZ+/OT+TJ8M30/NiPyCVBzA==")
spark.conf.set(
  "fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net",
  "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D")

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://epsilon-container@epsilonblobstore.blob.core.windows.net/media-breakdowns-new",
  mountPoint = "/mnt/media-breakdowns-new",
  extraConfigs = Map("fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net" -> "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://epsilon-container@epsilonblobstore.blob.core.windows.net/media-breakdowns-processed",
  mountPoint = "/mnt/media-breakdowns-processed",
  extraConfigs = Map("fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net" -> "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://epsilon-container@epsilonblobstore.blob.core.windows.net/epsilon-reviews",
  mountPoint = "/mnt/epsilon-reviews",
  extraConfigs = Map("fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net" -> "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://epsilon-container@epsilonblobstore.blob.core.windows.net/epsilon-dir",
  mountPoint = "/mnt/epsilon-dir",
  extraConfigs = Map("fs.azure.sas.epsilon-container.epsilonblobstore.blob.core.windows.net" -> "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2018-07-06T11:08:03Z&st=2018-02-06T03:08:03Z&spr=https&sig=4ROOYm1h32cZPcVi4s0XrgRVjWyHlBOcLB3PffC5%2BeI%3D"))

// COMMAND ----------

// MAGIC %fs refreshMounts

// COMMAND ----------

// MAGIC %fs mounts