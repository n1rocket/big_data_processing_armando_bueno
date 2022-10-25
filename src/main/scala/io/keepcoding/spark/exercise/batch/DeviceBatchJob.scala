package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.streaming.DeviceStreamingJob.{readFromKafka, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime
import scala.concurrent.Future

object DeviceBatchJob extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Final Exercise SQL Batch")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"$storagePath/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  override def readDeviceMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichDeviceWithMetadata(deviceDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    deviceDF.as("a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")
  }

//  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
//    dataFrame
//      .filter($"metric" === lit("devices_count"))
//      .select($"timestamp", $"location", $"value")
//      //No necesario para Batch //.withWatermark("timestamp", "10 seconds") //1 minute
//      .groupBy($"location", window($"timestamp", "1 hour")) //5 minutes
//      .agg(
//        avg($"value").as("avg_devices_count"), //mismo nombre que la tabla sql donde vamos a guardar (revisar provisioner)
//        max($"value").as("max_devices_count"),
//        min($"value").as("min_devices_count"),
//      )
//      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
//  }


  override def computeBytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .groupBy($"antenna_id", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"total_bytes".as("value"), lit("antenna_bytes_total").as("type"))
  }

  override def computeBytesByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes", $"email")
      .groupBy($"id", $"email", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"email".as("id"), $"total_bytes".as("value"), lit("email_total_bytes").as("type"))
  }

  override def computeBytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour").as("window"))
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"total_bytes".as("value"), lit("app_bytes_total").as("type"))

  }
  override def computeUserOverQuota(dataFrame: DataFrame): DataFrame = {
    // Email de usuarios que han sobrepasado la cuota por hora.
    // CREATE TABLE user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP);

    dataFrame
      .select($"timestamp", $"id", $"bytes", $"email", $"quota")
      .groupBy($"id", $"email", window($"timestamp", "1 hour").as("window"), $"quota")
      .agg(
        sum($"bytes").as("total_bytes"),
      )
      .filter($"total_bytes" > $"quota")
      .select($"email", $"total_bytes".as("usage"), $"quota", $"window.start".as("timestamp"))
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"$storageRootPath/historical")
  }

  def main(args: Array[String]): Unit = {
    val storage = "/Users/abueno"

    val jdbcUri = "jdbc:postgresql://34.173.106.255:5432/postgres"
    val jdbcUser = "postgres"
    val jdbcPassword = "keepcoding"

    val offsetDateTime = OffsetDateTime.parse("2022-10-24T19:00:00Z")

    val devicesDF = readFromStorage(s"$storage/tmp/devices_parquet/", offsetDateTime)
    val metadataDF = readDeviceMetadata(jdbcUri, "user_metadata", jdbcUser, jdbcPassword)
    val enrichedDevicesMetadataDF = enrichDeviceWithMetadata(devicesDF, metadataDF).cache()

    val aggByAntennaDF = computeBytesByAntenna(enrichedDevicesMetadataDF)
    val aggByUserDF = computeBytesByUser(enrichedDevicesMetadataDF)
    val aggByAppDF = computeBytesByApp(enrichedDevicesMetadataDF)
    val aggQuotaDF = computeUserOverQuota(enrichedDevicesMetadataDF)

    writeToJdbc(aggByAntennaDF, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggByUserDF, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggByAppDF, jdbcUri, "bytes_hourly", jdbcUser, jdbcPassword)
    writeToJdbc(aggQuotaDF, jdbcUri, "user_quota_limit", jdbcUser, jdbcPassword)

    //writeToStorage(enrichedDevicesMetadataDF, "/tmp/devices_parquet/")

    spark.close()

  }
}
