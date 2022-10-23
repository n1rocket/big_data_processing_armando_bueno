package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.streaming.DeviceStreamingJob.{readFromKafka, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime
import scala.concurrent.Future

object AntennaBatchJob extends BatchJob {
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

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")
  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"metric" === lit("devices_count"))
      .select($"timestamp", $"location", $"value")
      //No necesario para Batch //.withWatermark("timestamp", "10 seconds") //1 minute
      .groupBy($"location", window($"timestamp", "1 hour")) //5 minutes
      .agg(
        avg($"value").as("avg_devices_count"), //mismo nombre que la tabla sql donde vamos a guardar (revisar provisioner)
        max($"value").as("max_devices_count"),
        min($"value").as("min_devices_count"),
      )
      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

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

    val offsetDateTime = OffsetDateTime.parse("2022-10-21T23:00:00Z")

    val antennaDF = readFromStorage(s"$storage/tmp/antenna_parquet/", offsetDateTime)
    val metadataDF = readAntennaMetadata(jdbcUri, "metadata", jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF).cache()
    val aggByCoordinatesDF = computeDevicesCountByCoordinates(antennaMetadataDF)
    //val aggPercentStatusDF = computePercentStatusByID(antennaMetadataDF)
    //val aggErroAntennaDF = computeErrorAntennaByModelAndVersion(antennaMetadataDF)

    writeToJdbc(aggByCoordinatesDF, jdbcUri, "antenna_1h_agg", jdbcUser, jdbcPassword)
    //writeToJdbc(aggPercentStatusDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    //writeToJdbc(aggErroAntennaDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    writeToStorage(antennaDF, "/tmp/antenna_parquet/")

    spark.close()

  }
}
