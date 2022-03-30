# Databricks notebook source
#Passthrough auth (did not work for streaming)
configs = { 
"fs.azure.account.auth.type": "CustomAccessToken",
"fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

#Auth with SP (worked for streaming)
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "3e4741f0-980b-4a3d-9d69-1cbb47c1f266",
          "fs.azure.account.oauth2.client.secret": "5UU7Q~nXBmIs3WP2Z9Y32fOO-QwM0HPntljBY",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5b682058-571d-4784-97ba-42dd0f104902/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://productioncontainer@svcmadlsmsdn.dfs.core.windows.net/ambulancedatastream",
  mount_point = "/mnt/spambulancedata",
  extra_configs = configs)


# COMMAND ----------

#Passthrough auth mount
dbutils.fs.mount(
  source = "abfss://productioncontainer@svcmadlsmsdn.dfs.core.windows.net/ambulancedatastream",
  mount_point = "/mnt/ambulancedata",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.cp('abfss://productioncontainer@svcmadlsmsdn.dfs.core.windows.net/ambulancedata/vehicle2_09162014.csv', 'abfss://productioncontainer@svcmadlsmsdn.dfs.core.windows.net/ambulancedatastream/vehicle2_09162014.csv')

# COMMAND ----------

inputPath = "/mnt/spambulancedata"

# Define the schema to speed up processing
csvSchema = StructType([ StructField("c0", StringType(), True), StructField("c1", StringType(), True),StructField("c2", TimestampType(), True), StructField("c3", StringType(), True), StructField("c4", StringType(), True), StructField("c5", StringType(), True), StructField("c6", StringType(), True), StructField("c7", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(csvSchema)               
    .option("maxFilesPerTrigger", 1)
    .csv(inputPath)
)
streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.c2,
      )
    .count()
)

# COMMAND ----------

q1 = (streamingCountsDF.display())

# COMMAND ----------

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("totalcounts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

q1 = (
    streamingCountsDF.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "/mnt/spambulancedata/chkpoint")
    .start("/mnt/spambulancedata/delta")
)

# COMMAND ----------

# MAGIC %sql select count(*) from totalcounts group by count

# COMMAND ----------

# MAGIC %sql select * from delta1

# COMMAND ----------

q3 = (streamingCountsDF.writeStream
   .format("delta")
   #.load("/mnt/spambulancedata")
   #.groupBy("c1")
   #.count()
   #.writeStream
   .format("delta")
   .outputMode("complete")
   .option("checkpointLocation", "/mnt/spambulancedata/chkpointdelta1")
   .start("/mnt/spambulancedata/delta1")
  )
