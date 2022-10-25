package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readDeviceMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame


  def computeBytesByAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesByUser(dataFrame: DataFrame): DataFrame

  def computeBytesByApp(dataFrame: DataFrame): DataFrame

  def computeUserOverQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcQuotaTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readDeviceMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val enrichedDF = enrichDeviceWithMetadata(antennaDF, metadataDF).cache()
    val aggByAntenna = computeBytesByAntenna(enrichedDF)
    val aggByUser = computeBytesByUser(enrichedDF)
    val aggByApp = computeBytesByApp(enrichedDF)
    val aggByQuota = computeUserOverQuota(enrichedDF)

    writeToJdbc(aggByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByUser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByApp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByQuota, jdbcUri, aggJdbcQuotaTable, jdbcUser, jdbcPassword)

    writeToStorage(antennaDF, storagePath)

    spark.close()
  }

}
