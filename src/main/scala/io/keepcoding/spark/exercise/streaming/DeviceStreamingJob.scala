package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object DeviceStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Final Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("failOnDataLoss", "false")
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false),
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as("json"))
      .select("json.*")

  }


  override def readDevicesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDeviceWithMetadata(devicesDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    devicesDF.as("a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      ).drop($"b.id")
  }

  override def computeBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"antenna_id", window($"timestamp", "5 minutes").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"total_bytes".as("value"), lit("antenna_bytes_total").as("type"))
  }

  override def computeBytesByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"id", window($"timestamp", "5 minutes").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id".as("id"), $"total_bytes".as("value"), lit("user_bytes_total").as("type"))

  }

  override def computeBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"app", window($"timestamp", "5 minutes").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"total_bytes".as("value"),  lit("app_bytes_total").as("type"))

  }

  //Para que funcionen los futuros

  import scala.concurrent.ExecutionContext.Implicits.global

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch {
        (batchDF: DataFrame, idPage: Long) => {
          batchDF
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", jdbcURI)
            .option("dbtable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"timestamp", $"id", $"antenna_id", $"bytes", $"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour"),
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    //run(args)

    val storage = "/Users/abueno"
    val ipKafka = "34.125.140.105:9092"
    val jdbcUri = s"jdbc:postgresql://34.173.106.255:5432/postgres"
    val jdbcUser = "postgres"
    val jdbcPassword = "keepcoding"

    val kafkaDF = readFromKafka(ipKafka, "devices")
    val devicesDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(devicesDF, s"$storage/tmp/devices_parquet/")
    val metadataDF = readDevicesMetadata(jdbcUri, "user_metadata", jdbcUser, jdbcPassword)
    val enrichedDF = enrichDeviceWithMetadata(devicesDF, metadataDF)

    val aggBytesByAntennaDF = computeBytesByAntenna(enrichedDF)
    val aggBytesByUserDF = computeBytesByUser(enrichedDF)
    val aggBytesByAppDF = computeBytesByApp(enrichedDF)

    val antennaJdbcFuture = writeToJdbc(aggBytesByAntennaDF, jdbcUri, "bytes", jdbcUser, jdbcPassword)
    val userJdbcFuture = writeToJdbc(aggBytesByUserDF, jdbcUri, "bytes", jdbcUser, jdbcPassword)
    val appJdbcFuture = writeToJdbc(aggBytesByAppDF, jdbcUri, "bytes", jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(storageFuture, antennaJdbcFuture, userJdbcFuture, appJdbcFuture)), Duration.Inf)

//    aggs
//      .writeStream
//      .format("console")
//      .start()
//      .awaitTermination()

    spark.close()

  }
}
