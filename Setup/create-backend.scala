// Databricks notebook source
// DBTITLE 1,Histogram UDAF
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class HistogramOfFileSize extends UserDefinedAggregateFunction {

  override def inputSchema: StructType =
    StructType(StructField("size", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(
      StructField("lessThan1MB", LongType) ::
      StructField("lessThan10MB", LongType) ::
      StructField("lessThan128MB", LongType) ::
      StructField("lessThan1GB", LongType) ::
      StructField("greaterThan1GB", LongType) :: Nil
  )

  override def dataType: DataType = bufferSchema

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for (i <- 0 until bufferSchema.length) {
      buffer(i) = 0L
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (input.isNullAt(0)) {
      return
    }
    val size = input.getAs[Long](0)
    if (size < 1024L * 1024L) {
      buffer(0) =  buffer.getAs[Long](0) + 1L
    } else if (size < 10 * 1024L * 1024L) {
      buffer(1) =  buffer.getAs[Long](1) + 1L
    } else if (size < 128 * 1024L * 1024L) {
      buffer(2) =  buffer.getAs[Long](2) + 1L
    } else if (size < 1024 * 1024L * 1024L) {
      buffer(3) = buffer.getAs[Long](3) + 1L
    } else {
      buffer(4) = buffer.getAs[Long](4) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    for (i <- 0 until bufferSchema.length) {
      buffer1(i) = buffer1.getAs[Long](i) + buffer2.getAs[Long](i)
    }
  }

  override def evaluate(buffer: Row): Any = buffer
}

case class FileSizeStats(
  var lessThan1MB: Long = 0,
  var lessThan10MB: Long = 0,
  var lessThan128MB: Long = 0,
  var lessThan1GB: Long = 0,
  var greaterThan1GB: Long = 0
)

// COMMAND ----------

// DBTITLE 1,Data Structures
case class PartitionStats(
    tableName: String,
    tableFormat: String,
    tablePath: String,
    partitionPath: String,
    partitionValueMap: Map[String, String],
    numberFiles: Long,
    sizeBytes: Long,
    lastModified: Long,
    delta: DeltaStats,
    histogram: FileSizeHistogram,
    error: String
)

object PartitionStats {

def apply(tableName: String, path: String, error: Throwable) =
  new PartitionStats(tableName=tableName, tableFormat=null, tablePath=path, partitionPath=null, partitionValueMap=null, numberFiles=0, sizeBytes=0, lastModified=0, delta=null, histogram=null, error=error.toString)
}

case class FileSizeHistogram(
    lessThan1MB: Long,
    lessThan10MB: Long,
    lessThan128MB: Long,
    lessThan1GB: Long,
    greaterThan1GB: Long
)
case class DeltaStats(numFilesDataChanged: Long, numFilesZOrdered: Long, numFilesWithColumnStats: Long)

// COMMAND ----------

// DBTITLE 1,Delta Stats
import org.apache.spark.sql.types._
import scala.util.control.NonFatal
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import com.databricks.sql.transaction.tahoe._
import io.delta.tables._
import org.apache.hadoop.fs.Path

def deltaLogStats(path: String) = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val snapshot = deltaLog.snapshot
    val partitions = snapshot.allFiles
    val result = partitions
    result.groupBy($"partitionValues".cast("String").as("partitionValues")).agg(
      first(concat(lit(path), lit("/"), regexp_extract($"path", "^(.+)\\/([^\\/]+)$", 1), lit("/"))).as("partitionPath"),
      first("partitionValues").as("partitionValueMap"),
      count("*").as("numberFiles"),
      sum("size").as("sizeBytes"),
      max("modificationTime").as("lastModified"),
      struct(
        sum(when($"dataChange" === "true", 1).otherwise(0)).as("numFilesDataChanged"),
        sum(when($"tags.ZCUBE_ID".isNotNull, 1).otherwise(0)).as("numFilesZOrdered"),
        sum(when($"stats".isNotNull, 1).otherwise(0)).as("numFilesWithColumnStats")
        ).alias("delta"),
      histogramOfFileSize($"size").alias("histogram")
    ).drop("partitionValues")
}

val histogramOfFileSize = new HistogramOfFileSize

def deltaPartitionStatsPath(path: String) = {
spark
  .sql(s"DESCRIBE DETAIL delta.`$path`")
  .selectExpr(
    "name as tableName",
    "format as tableFormat",
    "location as tablePath"
  )
  .crossJoin(
    deltaLogStats(path))
  .withColumn("error", lit(null).cast("String")).as[PartitionStats]
}

def deltaPartitionStatsName(name: String) = {
  def getTableLocation(tableName: String) = {
    spark.sql(s"DESCRIBE DETAIL $tableName").select("location").collect()(0).getString(0)
  }
  spark.sql(s"DESCRIBE DETAIL $name")
    .selectExpr("name as tableName",
               "format as tableFormat",
               "location as tablePath")
  .crossJoin(
    deltaLogStats(getTableLocation(name)))
  .withColumn("error", lit(null).cast("String")).as[PartitionStats]
}

// COMMAND ----------

// DBTITLE 1,Non-Delta Stats
import scala.collection.mutable._

def gatherAllDirs(path: String): ArrayBuffer[com.databricks.backend.daemon.dbutils.FileInfo] = {
  val formatter = java.text.NumberFormat.getIntegerInstance
  var (bytes, count)  = (0L, 0L)

  import scala.collection.mutable.ArrayBuffer
  val files = ArrayBuffer(dbutils.fs.ls(path): _*).filter(x => !(x.path contains "_delta_log"))
  val result = ArrayBuffer(dbutils.fs.ls(path): _*).filter(x => !(x.path contains "_delta_log"))
  var depth =0L

  while (!files.isEmpty) {
    val fileInfo = files.remove(0)
    if (!fileInfo.isDir) {
        val count = fileInfo.path.count(_ == '=')
        if (depth < count) depth = count
            
      
    } else {
      files.append(dbutils.fs.ls(fileInfo.path): _*)
      result.append(dbutils.fs.ls(fileInfo.path): _*)
    }
  }
  result.filter(x => (x.isDir)).filter( x=> !( (x.path contains "_delta_log") || 
                                              (x.path contains "_SUCCESS") || 
                                              (x.path contains "_committed") || 
                                              (x.path contains "_started") ||
                                             (x.path contains "_db_metadata"))).filter(x => (x.path.count(_ == '=')) == depth)
}

def getFileSystemHistogram(sizes: ArrayBuffer[Long]) = {
  val lessThan1MB = sizes.filter(x => x < (1024 * 1024)).size
  val lessThan10MB = sizes.filter(x => x > ((1024 * 1024)) && x < (1024 * 1024 * 10)).size
  val lessThan128MB = sizes.filter(x => x > ((1024 * 1024 * 10)) && x < (1024 * 1024 * 128)).size
  val lessThan1GB = sizes.filter(x => x > ((1024 * 1024 * 128)) && x < (1024 * 1024 * 1024)).size
  val greatorThan1GB = sizes.filter(x => x > (1024 * 1024 * 1024)).size
  new FileSizeHistogram(lessThan1MB, lessThan10MB, lessThan128MB, lessThan1GB, greatorThan1GB)
}

def nonDeltaPartitionStats(tableName: String, path: String) = {
  val fullListOfFiles = ArrayBuffer(dbutils.fs.ls(path): _*)
  val isDelta = if(fullListOfFiles.filter(x => x.path contains "_delta_log").size >= 1) true else false
  val format = (if (isDelta) "delta" else if (fullListOfFiles
                         .filter(x => x.path contains ".parquet")
                         .size >= 1) "parquet"
                   else "other")
  val files = fullListOfFiles.filter(x =>
    !((x.path contains "_delta_log") ||
      (x.path contains "_SUCCESS") ||
      (x.path contains "_committed") ||
      (x.path contains "_started") ||
      (x.path contains "_db_metadata"))
  )
  val isPartitioned = (files.filter(x => x.isDir).size == files.size)
  if(isDelta) {
     deltaPartitionStatsPath(path)
  } else {
  if(isPartitioned) {
    var result =  ListBuffer[PartitionStats]()
    val fileInfos = gatherAllDirs(path)
    for (part <- fileInfos) {
      val partition = part.path
      val filesInPartition = ArrayBuffer(dbutils.fs.ls(partition): _*)
      var (bytes, count) = (0L, 0L)
      var sizes: ArrayBuffer[Long] = ArrayBuffer()
      while (!filesInPartition.isEmpty) {
        val fileInfo = filesInPartition.remove(0)
        if (!fileInfo.isDir) {
          count += 1
          bytes += fileInfo.size
          sizes += fileInfo.size
        } else {
          new PartitionStats(tableName=tableName, tableFormat=format, tablePath=path, partitionPath=partition,partitionValueMap=null,numberFiles=count, sizeBytes=0,lastModified=0,delta=null,histogram=null, error="malformed table exception")
        }
      }
      val histogram = getFileSystemHistogram(sizes)
      val partStats = PartitionStats(tableName=tableName, tableFormat=format, tablePath=path, partitionPath=partition,partitionValueMap=null,numberFiles=count, sizeBytes=bytes,lastModified=0,delta=null,histogram=histogram, error=null)
      result += partStats
    }
    result.toSeq.toDS
  } else {
         var result =  ListBuffer[PartitionStats]()        
         var (bytes, count)  = (0L, 0L)
         var sizes: ArrayBuffer[Long] = ArrayBuffer()
         while (!files.isEmpty) {
           val fileInfo = files.remove(0)
           if (!fileInfo.isDir) {
             count += 1
             bytes += fileInfo.size
             sizes += fileInfo.size
             } else {
              new PartitionStats(tableName=tableName, tableFormat=format, tablePath=path, partitionPath=path,partitionValueMap=null,numberFiles=count, sizeBytes=0,lastModified=0,delta=null,histogram=null, error=null)

           }
          }// end of while
        val histogram = getFileSystemHistogram(sizes)
        val partStats = PartitionStats(tableName=tableName, tableFormat=format, tablePath=path, partitionPath=path,partitionValueMap=null,numberFiles=count, sizeBytes=bytes,lastModified=0,delta=null,histogram=histogram, error=null)

          Seq(partStats).toDS
  }
  }
}

// COMMAND ----------

// DBTITLE 1,Meta-interface
def nameReport(tableName: String) = {
  try {
  val tableInfo = spark.sql(s"DESCRIBE DETAIL $tableName").select("format", "location").collect()(0)
  val format = tableInfo.getString(0)
  val location = tableInfo.getString(1)
  if(format == "delta") {
    deltaPartitionStatsName(tableName)
  }
  else {
    nonDeltaPartitionStats(tableName, location)
  }
  } catch {
    case NonFatal(e) => Seq(PartitionStats(tableName, null, e)).toDS
  }
}

def pathReport(path: String) = {
  try {
  nonDeltaPartitionStats(null, path)
  } catch {
    case NonFatal(e) => Seq(PartitionStats(null, path, e)).toDS
  }
}

// COMMAND ----------

// DBTITLE 1,User interface
def databaseNameReport(name: String) = {
  val tables_ds = spark.catalog.listTables(name).selectExpr("concat(database, '.', name) as table_name")
  val tables = tables_ds.select("*").as[String].collect()
  tables.map(x => nameReport(x)).reduce(_ union _)
}

def tableNameReport(name: String) = {
  nameReport(name)
}

def databasePathReport(path: String) = {
  dbutils.fs.ls(path).map(_.path).map(x => pathReport(x)).reduce(_ union _)
}

def tablePathReport(path: String) = {
  pathReport(path)
}

// COMMAND ----------

// gets params from frontend notebook, selects the right function and sends back a link to the global temp view
val medium = dbutils.widgets.get("medium")
val value = dbutils.widgets.get("value")

def matchTest(x: String) = x match {
  case "Database Name" => databaseNameReport _
  case "Database Path" => databasePathReport _
  case "Table Path" =>  tablePathReport _ 
  case "Table Name" => tableNameReport _
}

val df = matchTest(medium)(value)
df.createOrReplaceGlobalTempView("deltaeval")
dbutils.notebook.exit("deltaeval")

// COMMAND ----------

databaseNameReport("_databricks").show(2000,2000,true)

// COMMAND ----------

