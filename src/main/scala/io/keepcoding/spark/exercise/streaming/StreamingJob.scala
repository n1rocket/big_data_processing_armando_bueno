package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readDevicesMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesByAntenna(dataFrame: DataFrame): DataFrame
  def computeBytesByUser(dataFrame: DataFrame): DataFrame
  def computeBytesByApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val dataFromKafkaDF = parserJsonData(kafkaDF)
    val metadataDF = readDevicesMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val enrichedDF = enrichDeviceWithMetadata(dataFromKafkaDF, metadataDF)
    val storageFuture = writeToStorage(dataFromKafkaDF, storagePath)
    val aggBytesByAntennaDF = computeBytesByAntenna(enrichedDF)
    val aggBytesByUserDF = computeBytesByUser(enrichedDF)
    val aggBytesByAppDF = computeBytesByApp(enrichedDF)
    val aggFuture = writeToJdbc(aggBytesByAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(aggFuture, storageFuture)), Duration.Inf)

    spark.close()
  }

}
