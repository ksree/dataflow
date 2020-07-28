package com.ksr.dataflow.configuration.job.input

import com.ksr.dataflow.configuration.job.InputConfig
import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.readers.kafka.KafkaInput

case class Kafka(config: String,
                 topic: Option[String],
                 topicPattern: Option[String],
                 consumerGroup: Option[String],
                 options: Option[Map[String, String]],
                 schemaRegistryUrl:  Option[String],
                 schemaSubject:  Option[String],
                 schemaId: Option[String]
                ) extends InputConfig {
  require(Option(config).isDefined, "Kafka Config file must be provided")
  require(topic.isDefined && !topicPattern.isDefined || !topic.isDefined &&
    topicPattern.isDefined &&
    schemaSubject.isDefined,
    "Exactly one of (topic, topicPattern) must be defined")

  override def getReader(name: String): Reader = KafkaInput(name, config, topic, topicPattern, consumerGroup, options,
    schemaRegistryUrl, schemaSubject, schemaId)
}

