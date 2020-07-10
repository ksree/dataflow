package com.ksr.dataflow.configuration

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Configuration(inputs: Option[Map[String, Input]],
                         transformations: Option[Seq[String]],
                         outputs: Option[List[Output]],
                         logLevel: Option[String],
                         showPreviewLines: Option[Int],
                         appName: Option[String])

case class Input(file: Option[File],
                 jdbc: Option[JDBC],
                 kafka: Option[Kafka],
                 cassandra: Option[Cassandra],
                 elasticsearch: Option[Elasticsearch],
                 mongo: Option[MongoDB]) extends InputConfig {
  def getReader(name: String): Reader = {
    Seq(file, fileDateRange, jdbc, kafka, cassandra, elasticsearch, mongo).find(
      x => x.isDefined
    ).get.get.getReader(name)
  }
}
case class Output(name: String, format: String, path: String)

object Configuration {
  def apply(configPath: String): Configuration = {
    val objectMapper = new ObjectMapper(new YAMLFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue(new File(configPath), classOf[Configuration])
  }
}