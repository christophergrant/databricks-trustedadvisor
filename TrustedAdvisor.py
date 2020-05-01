# Databricks notebook source
# MAGIC %md
# MAGIC # Trusted Advisor
# MAGIC 
# MAGIC 
# MAGIC Welcome to the technical notebook of the Trusted Advisor program! This notebook will offer a technical look at your data lake, including diagnostics, example architectures, and incoming features.
# MAGIC 
# MAGIC The overall goal of Trusted Advisor is to give insights and empower users to optimize their data lake. This could come in the following forms: switching from Parquet to Delta in-place, running optimizations like `OPTIMIZE` and `ZORDER` for **faster query performance** and **lower compute costs**, or re-architecting a pipeline for lower code complexity and maintenance.

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO
# MAGIC - add recommendation for z-order on Largest Partitions Across Tables - With File Size Distribution
# MAGIC - use modified for recommendation
# MAGIC - Chris: 
# MAGIC   - work on modified timestamp for non-delta
# MAGIC   - persist table
# MAGIC   - ~~add collected table statistics to source~~
# MAGIC   - merge on partition path and modified timestamp

# COMMAND ----------

# DBTITLE 1,Get Report Parameters - fill out widgets at the top!
dbutils.widgets.dropdown("medium", "Database Name", ["Database Name", "Database Path", "Table Path", "Table Name"], label = "Target Type")
dbutils.widgets.text("value", defaultValue="", label='Target Value (path or name)')

# COMMAND ----------

# DBTITLE 1,Setup Report Data - this can take a bit of time depending on the number of tables
params = {"medium": dbutils.widgets.get("medium"), "value": dbutils.widgets.get("value")}
returned_table = dbutils.notebook.run("./Setup/create-backend", timeout_seconds= 172799, arguments = params) # timeout_seconds max is 48 hours, 48 hours - 1 second = 172799 seconds to be safe
global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")
# note, not actually saving the table, just using a global temp 
df = table(global_temp_db + "." + returned_table)
df.cache()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Distribution of Table Formats in your Data Lake

# COMMAND ----------

# DBTITLE 1,Table Format Report
# MAGIC %sql
# MAGIC SELECT CASE WHEN tableFormat IS NULL THEN "error"
# MAGIC             WHEN tableFormat = "other" THEN "other (csv, json, etc.)"
# MAGIC             ELSE tableFormat 
# MAGIC             END AS tableFormat, count(*)
# MAGIC FROM (SELECT tableFormat, tablePath
# MAGIC   FROM global_temp.deltaeval
# MAGIC   GROUP BY tableFormat, tablePath)
# MAGIC GROUP BY tableFormat

# COMMAND ----------

# DBTITLE 1,Largest Tables - With Recommendations
# MAGIC %sql
# MAGIC -- TODO: make recommendation tablename or delta.`` syntax
# MAGIC SELECT tablePath, sum(histogram.lessThan1MB) as lessThan1MB, sum(histogram.lessThan10MB) as 1M_to_10MB, sum(histogram.lessThan128MB) as 10MB_to_128MB, sum(histogram.lessThan1GB) as 128MB_to_1GB, sum(histogram.greaterThan1GB) as greaterThan1GB, concat(round(sum(sizeBytes) / 1000000000, 2), " GB") as totalSizeGB, CASE WHEN sum(sizeBytes) > 10000000000 THEN CONCAT("OPTIMIZE ", "delta.`", tablePath, "`") ELSE "" END as recommendation
# MAGIC FROM global_temp.deltaeval
# MAGIC GROUP BY tablePath
# MAGIC ORDER BY lessThan1MB DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Large, unpartitioned tables
# MAGIC %sql
# MAGIC -- TODO: add path
# MAGIC SELECT *
# MAGIC   FROM (SELECT tablePath, tableFormat, count(partitionPath) as numOfPartitions, sum(sizeBytes) / 1000000000 as totalSizeGB
# MAGIC     FROM global_temp.deltaeval
# MAGIC     GROUP BY tableFormat, tablePath
# MAGIC   )
# MAGIC   WHERE numOfPartitions < 2 AND totalSizeGB > 1
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Largest Partitions Across Tables - With File Size Distribution
# MAGIC %sql
# MAGIC SELECT tablePath, partitionPath, tableFormat,
# MAGIC     histogram.lessThan1MB as lessThan1MB,
# MAGIC     histogram.lessThan10MB as 10MB_to_128MB,
# MAGIC     histogram.lessThan128MB + 
# MAGIC     histogram.lessThan1GB as 128MB_to_1GB,
# MAGIC     histogram.greaterThan1GB,
# MAGIC     round(sizeBytes / 1000000000, 2) as totalSizeGB,
# MAGIC     CASE
# MAGIC       WHEN tableFormat = 'delta' AND map_keys(partitionValueMap)[0] IS NOT NULL
# MAGIC         THEN CONCAT("OPTIMIZE ", "delta.`", tablePath, "`", ' WHERE ', map_keys(partitionValueMap)[0], ' = \'', map_values(partitionValueMap)[0], '\'')
# MAGIC       WHEN tableFormat = 'delta'
# MAGIC         THEN CONCAT("OPTIMIZE ", "delta.`", tablePath, "`")
# MAGIC       ELSE '' END
# MAGIC       AS recommendation
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE histogram IS NOT NULL
# MAGIC ORDER BY sizeBytes DESC

# COMMAND ----------

# DBTITLE 1,Table Partition Cardinality
# MAGIC %sql
# MAGIC SELECT tablePath, tableFormat, count(partitionPath) as numOfPartitions
# MAGIC FROM global_temp.deltaeval
# MAGIC GROUP BY tableFormat, tablePath
# MAGIC ORDER BY numOfPartitions DESC

# COMMAND ----------

# DBTITLE 1,Converting Parquet tables to Delta - Copy and Paste commands
# MAGIC %sql
# MAGIC SELECT concat('CONVERT TO DELTA ', "parquet.`", tablePath, "`", CASE WHEN partitionNames IS NOT NULL AND partitionNames <> "" THEN concat(' PARTITIONED BY ', partitionNames) ELSE '' END) AS command
# MAGIC FROM (
# MAGIC   SELECT tablePath, concat_ws(', ', collect_set(map_keys(partitionValueMap))[0]) AS partitionNames
# MAGIC   FROM global_temp.deltaeval
# MAGIC   WHERE tableFormat = "parquet"
# MAGIC   GROUP BY tablePath 
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,Delta - Tables without Z-Order optimization
# MAGIC %sql
# MAGIC SELECT tablePath, sum(delta.numFilesZOrdered) / sum(numberFiles) as percentZOrdered, sum(sizeBytes) as totalSizeBytes, first(tablePath) as tablePath
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tableFormat = "delta"
# MAGIC GROUP BY tablePath
# MAGIC ORDER BY percentZOrdered ASC, totalSizeBytes DESC

# COMMAND ----------

# DBTITLE 1,Delta - Tables without collected statistics
# MAGIC %sql
# MAGIC SELECT tablePath, sum(delta.numFilesWithColumnStats) / sum(numberFiles) * 100 AS percentFilesWithColumnStats, sum(sizeBytes) AS totalSizeBytes, first(tableName) AS tableName
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tableFormat = "delta"
# MAGIC GROUP BY tablePath
# MAGIC HAVING percentFilesWithColumnStats < 100
# MAGIC ORDER BY percentFilesWithColumnStats ASC, totalSizeBytes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Single Table Details

# COMMAND ----------

# DBTITLE 1,Setup Table Details Widget
from pyspark.sql.functions import *
paths = [(row['tablePath']) for row in spark.table("global_temp.deltaeval").select("tablePath").distinct().dropna().collect()]
largestTablePathDf = (spark.table("global_temp.deltaeval")
  .groupBy("tablePath")
  .agg(sum(col("sizeBytes")).alias("totalSizeBytes"))
  .sort(desc("totalSizeBytes"))
  .limit(1)
)
largestTablePath = [(row['tablePath']) for row in largestTablePathDf.collect()][0]
dbutils.widgets.dropdown("tableDetails", largestTablePath, paths, "z_Table Details")

# COMMAND ----------

# DBTITLE 1,Table Summary
# MAGIC %sql
# MAGIC SELECT tableName, tablePath, tableFormat, count(*) as numOfPartitions, sum(sizeBytes) AS totalSizeBytes, concat(round(sum(sizeBytes) / 1000000000, 2), " GB") AS totalSizeGB, concat_ws(', ', collect_set(map_keys(partitionValueMap))[0]) AS partitionedBy
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tablePath = getArgument("tableDetails")
# MAGIC GROUP BY tableName, tablePath, tableFormat 

# COMMAND ----------

# DBTITLE 1,Biggest Partitions
# MAGIC %sql
# MAGIC SELECT partitionPath, sizeBytes, concat(round(sizeBytes / 1000000, 2), " MB") AS totalSizeMB
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tablePath = getArgument("tableDetails")
# MAGIC ORDER BY sizeBytes DESC

# COMMAND ----------

# DBTITLE 1,Table Summary - Partition File Sizes
# MAGIC %sql
# MAGIC SELECT tablePath, sum(lessThan1MB) as lessThan1MB, sum(10MB_to_128MB) as 10MB_to_128MB, sum(128MB_to_1GB) as 128MB_to_1GB, sum(greaterThan1GB) as greaterThan1GB, concat(round(sum(sizeBytes) / 1000000000, 2), " GB") as totalSizeGB, CASE WHEN sum(sizeBytes) > 10000000000 THEN CONCAT("OPTIMIZE ", tablePath) ELSE "" END as recommendation
# MAGIC FROM (
# MAGIC   SELECT tablePath, histogram.lessThan1MB as lessThan1MB,
# MAGIC     histogram.lessThan10MB as 10MB_to_128MB,
# MAGIC     histogram.lessThan128MB + 
# MAGIC     histogram.lessThan1GB as 128MB_to_1GB,
# MAGIC     histogram.greaterThan1GB,
# MAGIC     sizeBytes,
# MAGIC   CASE WHEN sizeBytes > 10000000000 THEN CONCAT("OPTIMIZE ", tablePath) ELSE "" END as recommendation
# MAGIC   FROM global_temp.deltaeval
# MAGIC   WHERE tablePath = getArgument("tableDetails") AND histogram IS NOT NULL
# MAGIC   )
# MAGIC GROUP BY tablePath
# MAGIC ORDER BY lessThan1MB DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Partition File Size Distribution - Largest Partitions
# MAGIC %sql
# MAGIC SELECT partitionPath, sizeBytes, histogram.lessThan1MB, histogram.lessThan10MB, histogram.lessThan128MB, histogram.lessThan1GB, histogram.greaterThan1GB
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tablePath = getArgument("tableDetails") AND histogram IS NOT NULL
# MAGIC ORDER BY sizeBytes DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Table Summary - Delta
# MAGIC %sql
# MAGIC SELECT tableName, tablePath, sum(delta.numFilesWithColumnStats) / sum(numberFiles) * 100 AS percentFilesWithColumnStats, sum(delta.numFilesZOrdered) / sum(numberFiles) * 100 AS percentFilesWithZOrder, sum(delta.numFilesDataChanged) / sum(numberFiles) * 100 AS percentFilesDataChanged
# MAGIC FROM global_temp.deltaeval
# MAGIC WHERE tablePath = getArgument("tableDetails") AND tableFormat = "delta"
# MAGIC GROUP BY tableName, tablePath, tableFormat 

# COMMAND ----------

# MAGIC %md
# MAGIC # Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC - Analyze current state of your lake
# MAGIC - Migrate use cases from Parquet to Delta
# MAGIC - Do a BVA on these use cases led by Databricks
# MAGIC - Delta Workshop Training
# MAGIC - Benchmark a migration to Delta
# MAGIC - Work with us

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional Info and Resources

# COMMAND ----------

# DBTITLE 1,Small Files - Top Offenders
# MAGIC %sql
# MAGIC SELECT tablePath, sum(lessThan1MB) as lessThan1MB, sum(10MB_to_128MB) as 10MB_to_128MB, sum(128MB_to_1GB) as 128MB_to_1GB, sum(greaterThan1GB) as greaterThan1GB, sum(sizeBytes) as totalSizeBytes, CASE WHEN sum(sizeBytes) > 50000000000 THEN CONCAT("OPTIMIZE ", tablePath) ELSE "" END as recommendation
# MAGIC FROM (
# MAGIC   SELECT tablePath, histogram.lessThan1MB as lessThan1MB,
# MAGIC     histogram.lessThan10MB as 10MB_to_128MB,
# MAGIC     histogram.lessThan128MB + 
# MAGIC     histogram.lessThan1GB as 128MB_to_1GB,
# MAGIC     histogram.greaterThan1GB,
# MAGIC     sizeBytes,
# MAGIC   CASE WHEN sizeBytes > 50000000000 THEN CONCAT("OPTIMIZE ", tablePath) ELSE "" END as recommendation
# MAGIC   FROM global_temp.deltaeval
# MAGIC   WHERE histogram IS NOT NULL
# MAGIC   )
# MAGIC GROUP BY tablePath
# MAGIC ORDER BY lessThan1MB DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What is Delta?
# MAGIC #### How does Delta affect my workflows?
# MAGIC Delta gives you the ability to do a lot of things that vanilla file formats lack, like time travel, performant data lake updates/deletes, and seamless unification of batch and streaming workflows.
# MAGIC 
# MAGIC ##### CDC (Change Data Capture)
# MAGIC CDC on Data Lakes has traditionally been tough due to the immutable nature of files on distributed file systems like HDFS, S3, and ADLS. Typically, you could go two ways:
# MAGIC * Try to perform CDC with Spark, Hive, etc directly on the Data Lake with a homegrown solution. This gives us a few problems:
# MAGIC   * These solutions are usually not performant and require partition or even full table rewrites. This can be expensive on cloud and otherwise inefficient. We can do better.
# MAGIC   * Homegrown solutions typically do not consider isolation, which means that data teams would typically have to perform CDC during off-peak hours or weekends, or risk queries breaking during the process of changing data
# MAGIC * Load data into a cloud data warehouse, perform CDC there and move the data back to DFS.
# MAGIC   * This can be very costly, monetarily, and also in terms of performance and pipeline complexity.
# MAGIC 
# MAGIC Delta offers the capability to do updates and deletes, all with full ACID compliance. It does not depend on the metastore for file locations, instead, it uses an internal Delta log. This can be especially helpful for Terabyte+ scale CDC workflows that would usually require many concurrent metastore connections. Instead, the Delta log acts as our metastore and is much more scalable. Delta also provides handy DMLs like `MERGE`, which makes writing CDC applications even easier. If you are using Apache Spark with Delta, you can easily perform batch and streaming CDC with minimal code difference between the two modes.
# MAGIC 
# MAGIC [Example code here](https://docs.delta.io/latest/delta-update.html#write-change-data-into-a-delta-table)
# MAGIC 
# MAGIC ##### Streaming Upserts and Deduplication
# MAGIC 
# MAGIC A common ETL use case is to collect logs into a Delta table by appending them to a table. However, often the sources can generate duplicate log records and downstream deduplication steps are needed to take care of them. With `MERGE`, you can avoid inserting duplicate records. All of this is guaranteed to maintain "exactly-once" semantics with more than one stream (with [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)) and/or batch, meaning that the resulting dataset will be consistently correct.
# MAGIC 
# MAGIC [Example code here](https://docs.delta.io/latest/delta-update.html#upsert-from-streaming-queries-using-foreachbatch)
# MAGIC 
# MAGIC ##### Streaming and Batch, Unified
# MAGIC 
# MAGIC You may have data sources that come at different intervals, perhaps one which delivers data quickly in a stream, also batching final results for reconciliation later. Before Delta, most attempts at joining the sink for batch and streaming together were referred to as the [Lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture). The lambda architecture was highly complex to implement and came with the overhead of maintaining two different modes of dataflow.
# MAGIC 
# MAGIC Delta frees us from the complexity of the Lambda architecture, offering a solution which gives all of the benefits of Lambda, without any of the downsides.
# MAGIC 
# MAGIC 
# MAGIC ##### GDPR/CCPA
# MAGIC Privacy legislation like [GDPR](https://gdpr-info.eu/) and [CCPA](https://en.wikipedia.org/wiki/California_Consumer_Privacy_Act) requires companies to be able to comply with a few kinds of data privacy requests, for example, to purge a specific person's data. In many ways, this is like the CDC use case, but instead of taking updates and deletes from an OLTP database, you are taking delete requests from customers. You can perform these deletes in a performant and audited manner, as well.
# MAGIC 
# MAGIC <a href="$./Resources/GDPR/gdpr_story">Link to notebook in same folder as current notebook</a>
# MAGIC ### How do I switch my workflows to Delta?
# MAGIC 
# MAGIC Switching workflows to Delta requires a few considerations:
# MAGIC * Changing table formats - If you **are using Parquet**, you can perform the conversion in-place *(see the below cell for EZ copypasta commands)*. All the conversion will do is create a delta log and collect statistics on your Parquet dataset. If you are **not using Parquet**, you will need to perform a full re-write, using Spark, Hive, Presto, etc. First, read the data as its current format and write it out temporarily as Delta. Finally, replace the original table with the temporary Delta table, completing the replacement.
# MAGIC 
# MAGIC * Changing code
# MAGIC   * Spark [read more here](https://docs.delta.io/latest/porting.html)
# MAGIC   * For analytics, querying the data with Spark, Hive, Presto, or Athena can be done using a connector.

# COMMAND ----------

# DBTITLE 1,Table Formats: Additional Detail
# MAGIC %md
# MAGIC 
# MAGIC <a href="$./Resources/fileFormats/fileFormats">Click here </a> for an explanatory notebook about table (file) formats

# COMMAND ----------

# DBTITLE 1,Delta - Unoptimized and neglected tables
# MAGIC %md
# MAGIC 
# MAGIC ### Delta - Optimization Reports
# MAGIC 
# MAGIC This report will show tables which are lacking proper optimization including:
# MAGIC * Delta tables which have not been `OPTIMIZE`ed
# MAGIC * Delta tables which have a significant amount of non-Z-ordered files
# MAGIC * Unpartitioned tables (including non-Delta)
# MAGIC 
# MAGIC On Delta tables, optimizations can be run at any time without any disruption in operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Best Practices
# MAGIC 
# MAGIC This will explain how we can use the above reports to affect our data lake's performance.
# MAGIC 
# MAGIC #### Interacting with Delta Lakes
# MAGIC 
# MAGIC Only use supported execution engines to interact with Delta Lake files. You will experience adverse affects if you attempt to change or delete any part of the underlying filestructure.
# MAGIC 
# MAGIC At this time, Apache Spark, Apache Hive**\***, and Apache Presto**\*** are all supported.
# MAGIC 
# MAGIC Removing underlying files should only be done with `VACUUM`, and not by deleting files in the distributed file system in which they are located.
# MAGIC 
# MAGIC \* *Apache Hive and Apache Presto connectors are available [here](https://github.com/delta-io/connectors) with the `GENERATE SYMLINK` syntax*
# MAGIC 
# MAGIC #### Bin-packing
# MAGIC 
# MAGIC Bin-packing describes the combination/compaction of small files into lesser large files, referring to those large files as "bins". Each bin has a target size, usually between 128MB - 1GB each, where multiple files are combined into single, large files. Note that bin-packing targets are related to file size and not the number of rows of data.
# MAGIC 
# MAGIC `OPTIMIZE` on Databricks is the easiest way to do bin-packing. `OPTIMIZE` is idempotent, meaning that if `OPTIMIZE` is run on an already optimized dataset, nothing will happen.
# MAGIC 
# MAGIC `OPTIMIZE` returns the file statistics (min, max, total, and so on) for the files removed and the files added by the operation. `OPTIMIZE` stats also contains the Z-Ordering statistics, the number of batches, and partitions optimized.
# MAGIC 
# MAGIC Boilerplate code:
# MAGIC `OPTIMIZE <table>`
# MAGIC 
# MAGIC #### Data Skipping
# MAGIC 
# MAGIC The easiest way to improve query performance on large datasets is to ensure that **only** the required data is loaded. Reading data is an inherently expensive operation, so avoiding reading unnecessary data will give us performance benefits. Here are ways to better organize data to ensure data is skipped appropriately:
# MAGIC 
# MAGIC ##### Partitioning
# MAGIC 
# MAGIC ###### What
# MAGIC 
# MAGIC Partitioning refers to the physical layout of data as it is stored at rest in a distributed filesystem like S3, ADLS, or HDFS. *It is not to be confused with Spark's shuffle partitions.*
# MAGIC 
# MAGIC Choosing the correct partition columns can be very impactful for performance due to a mechanism called *partition pruning*. What partition pruning does is skip entire directories of data. This is much more performant than scanning individual files, and is even cheaper than opening files because partition values can be gleaned from directory metadata.
# MAGIC 
# MAGIC 
# MAGIC ###### When
# MAGIC 
# MAGIC Partitioning of tables is done at table creation: [SQL syntax](https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html), [Examples for all languages](https://spark.apache.org/docs/2.2.2/sql-programming-guide.html#bucketing-sorting-and-partitioning)
# MAGIC 
# MAGIC ###### How
# MAGIC 
# MAGIC Here are some guidelines for partitioning:
# MAGIC * Choose columns that are often used in query predicates
# MAGIC   * If analysts are often slicing the data into different segments while querying, partitioning along those segments can vastly improve query performance
# MAGIC * Choose relatively low-medium cardinality columns for partitioning
# MAGIC   * A valid strategy could be partitioning on "event date", yielding one partition per day.
# MAGIC 
# MAGIC ##### Z-ORDERing
# MAGIC 
# MAGIC ###### What
# MAGIC 
# MAGIC [Z-Ordering](https://docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering) is a file-level optimization available on Databricks which co-locates data among files. This co-locality is automatically used by Spark compatible data-skipping algorithms to dramatically reduce the amount of data that needs to be read.
# MAGIC 
# MAGIC ###### How
# MAGIC 
# MAGIC To Z-Order data, you specify the columns to order on in the `OPTIMIZE` commands' `ZORDER BY` clause. 
# MAGIC 
# MAGIC ###### When
# MAGIC 
# MAGIC * If you expect a column to be commonly used in query predicates and if that column has high cardinality (that is, a large number of distinct values), then use ZORDER BY.
# MAGIC * You can specify multiple columns for ZORDER BY as a comma-separated list. However, the effectiveness of the locality drops with each additional column. Z-Ordering on columns that do not have statistics collected on them would be ineffective and a waste of resources as data skipping requires column-local stats such as min, max, and count. You can configure statistics collection on certain columns by re-ordering columns in the schema or increasing the number of columns to collect statistics on.
# MAGIC * Run Z-ORDER optimizations on data that is at rest and fully accounted for. Z-Ordering is done incrementally, but is much more effective when all of the required data is present. For example, let's say that we have a stream source, and we get late arriving records. It would make sense to wait until the next day to run a daily `OPTIMIZE` job which Z-Orders the previous day's data, because Z-Ordering can be an expensive (albeit worthwhile) operation. Making sure that most of the data is there when Z-Ordering will help cut costs while maintaining high data locality with Z-Ordering.
# MAGIC *Boilerplate code*: ```OPTIMIZE <table> ZORDER BY (<col1>)```
# MAGIC 
# MAGIC ##### Statistics
# MAGIC 
# MAGIC ###### What
# MAGIC Delta collects statistics on each file, including total record count, min, max, and number of distinct values and stores it in the Delta log. When accessing a Delta table, these statistics are used to filter files out of query consideration, making it so that if the file definitely does not have any information pertinent to a query, it will not be loaded into memory upon query execution.
# MAGIC 
# MAGIC By default, only the first 32 columns have stats collected on them.
# MAGIC 
# MAGIC ###### When
# MAGIC These statistics are collected automatically. There are rare situations where stats collection can adversely affect performance, particularly on large and low-latency data ingestion. For almost all use-cases, however, you will want this feature to be supplementing any optimizations you are already using.
# MAGIC 
# MAGIC ###### How
# MAGIC When converting to Delta, you have the option to not initially collect stats on the new Delta table. This is enabled due to performance considerations for extremely large tables at conversion time. To get stats on a table without statistics, simply run an `OPTIMIZE` command.
# MAGIC 
# MAGIC #### Cloud - Best instance types per workload type
# MAGIC 
# MAGIC <a href="$./Resources/instanceTypes/general_instancetypes_per_workload">General advice (notebook)</a>

# COMMAND ----------

# MAGIC %md
# MAGIC # Roadmap

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta Auto Data Loader (TBD)
# MAGIC ![alt_text](https://i.imgur.com/8RnmL4P.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta Pipelines (Q2 2020)
# MAGIC ![alt_text](https://i.imgur.com/0MixLMK.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Expectations (Q2 2020)
# MAGIC 
# MAGIC ![](https://i.imgur.com/PWSZ4YC.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Other incoming features
# MAGIC * `MERGE INTO` schema evolution (Q2 2020)
# MAGIC   * When source data is missing values, fill with null values for insertions, and leave unchanged for updates
# MAGIC   * When source data contains new columns, operations should automatically evolve the Delta table's schema to include the new columns, subject to existing restrictions on Delta schema evolution
# MAGIC   * However, queries which explicitly reference columns that do not exist in the target remain disallowed
# MAGIC 
# MAGIC * `MERGE INTO` performance improvements (2020)
# MAGIC   * Use probabilistic data structures like bloom filters to improve `MERGE` performance
# MAGIC 
# MAGIC * Easier access to semi-structured data (2020)
# MAGIC   * Dot notation syntax for accessing nested structures
# MAGIC   * Automatic parsing and storing of nested fields in Delta to avoid JSON deserialization costs at query time
# MAGIC    * Statistics collection on columns as they are being parsed to allow for optimizations like data skipping

# COMMAND ----------

# MAGIC %md
# MAGIC # Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC # Medallian (Delta) Architecture
# MAGIC ![](https://i.imgur.com/4dfY1i1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Community Links
# MAGIC 
# MAGIC [Delta website](https://delta.io/) - the official website for Open Source Delta Lake
# MAGIC 
# MAGIC [Delta users Slack](https://join.slack.com/t/delta-users/shared_invite/enQtODQ5ODM5OTAxMjAwLWY4NGI5ZmQ3Y2JmMjZjYjc1MDkwNTA5YTQ4MzhjOWY1MmVjNTM2OGZhNTExNmM5MzQ0YzEzZjIwMjc0OGI0OGM) - the official Delta Users Slack channel, for posing questions and everything Delta Lake
# MAGIC 
# MAGIC [Delta google group](https://groups.google.com/forum/#!forum/delta-users) - a mailing list for official discourse regarding Open Source Delta Lake
# MAGIC 
# MAGIC [Delta Github](https://github.com/delta-io/delta) - the code behind the magic, all Open Source!

# COMMAND ----------

