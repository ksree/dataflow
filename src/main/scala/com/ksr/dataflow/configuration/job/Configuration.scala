package com.ksr.dataflow.configuration.job

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ksr.dataflow.configuration.job.input.Input
import com.ksr.dataflow.input.Reader

case class Configuration(inputs: Option[Map[String, Input]],
                         transformations: Option[Seq[String]],
                         outputs: Option[List[Output]],
                         cacheOnPreview: Option[Boolean],
                         showQuery: Option[Boolean],
                         logLevel: Option[String],
                         showPreviewLines: Option[Int],
                         appName: Option[String],
                         var continueOnFailedStep: Option[Boolean],
                         var cacheCountOnOutput: Option[Boolean]) {
  def getReaders: Seq[Reader] = inputs.getOrElse(Map()).map {
    case (name, input) => input.getReader(name)
  }.toSeq
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