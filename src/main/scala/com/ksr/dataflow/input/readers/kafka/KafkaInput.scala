package com.ksr.dataflow.input.readers.kafka

import java.util.Properties

import com.ksr.dataflow.exceptions.DataFlowException
import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.utils.FileUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.abris.avro.functions.from_confluent_avro
import za.co.absa.abris.avro.read.confluent.SchemaManager

case class KafkaInput(name: String, config: String, topic: Option[String], topicPattern: Option[String], consumerGroup: Option[String],
                      options: Option[Map[String, String]], schemaRegistryUrl: Option[String], schemaSubject: Option[String],
                      schemaId: Option[String]) extends Reader {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)
  val props: Properties = FileUtils.loadConfig(config)


  def read(sparkSession: SparkSession): DataFrame = {
    val topicName: String = topic match {
      case Some(regular_topic) => regular_topic
      case _ => topicPattern match {
        case Some(regex_topic) => regex_topic
        case _ => throw DataFlowException("Please provide valid topic name")
      }
    }
    val reader = sparkSession.readStream
      .format(source = "org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", props.getProperty("bootstrap.servers"))
      .option("subscribe", topicName)
      .option("kafka.security.protocol", props.getProperty("security.protocol"))
      .option("kafka.sasl.mechanism", props.getProperty("sasl.mechanism"))
      .option("kafka.sasl.jaas.config", props.getProperty("sasl.jaas.config"))
      .option("kafka.ssl.endpoint.identification.algorithm", props.getProperty("ssl.endpoint.identification.algorithm"))
      .option("startingoffsets", "earliest")

    val inputStream: DataStreamReader = options match {
      case Some(x: Map[String, String]) => reader.options(options.get)
      case _ => reader
    }
    schemaRegistryUrl match {
      case Some(url) => {
        val schemaRegistryMap = getSchemaRegistryConfig(url, schemaSubject.getOrElse(topic.get), None)
        inputStream.load().select(from_confluent_avro(col("value"), schemaRegistryMap) as "result").select("result.*")
      }
      case None => inputStream.load()
    }
  }

  private def getSchemaRegistryConfig(schemaRegistryUrl: String, schemaRegistryTopic: String, schemaId: Option[String]): Map[String, String] = {
    val schemaIdValue = schemaId.getOrElse("latest")
    val schemaRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> schemaRegistryTopic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> schemaIdValue
    )
    val securityRegistryConfig =
      Map("basic.auth.credentials.source" -> "USER_INFO",
        "basic.auth.user.info" -> props.getProperty("schema.registry.basic.auth.user.info"))
    schemaRegistryConfig ++ securityRegistryConfig
  }
}