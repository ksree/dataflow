package com.ksr.dataflow.configuration.transform

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration


case class Output(name: Option[String],
                  dataFrameName: String,
                  @JsonScalaEnumeration(classOf[OutputTypeReference]) outputType: OutputType.OutputType,
                  reportLag: Option[Boolean],
                  reportLagTimeColumn: Option[String],
                  reportLagTimeColumnUnits: Option[String],
                  repartition: Option[Int],
                  coalesce: Option[Boolean],
                  protectFromEmptyOutput: Option[Boolean],
                  outputOptions: Map[String, Any])

object OutputType extends Enumeration {
  type OutputType = Value

  val Parquet,
  Cassandra,
  CSV,
  JSON,
  JDBC,
  File,
  GCPBigQuery,
  AzureSQL,
  AWSRedshift,
  Kafka = Value

}

class OutputTypeReference extends TypeReference[OutputType.type]
