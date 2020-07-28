package com.ksr.dataflow.configuration.job

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ksr.dataflow.input.Reader

case class Configuration(inputs: Option[Map[String, Input]],
                         transformations: Option[Seq[String]],
                         output: Option[Output],
                         outputs: Option[Map[String, Output]],
                         cacheOnPreview: Option[Boolean],
                         showQuery: Option[Boolean],
                         streaming: Option[Streaming],
                         logLevel: Option[String],
                         showPreviewLines: Option[Int],
                         appName: Option[String],
                         var continueOnFailedStep: Option[Boolean],
                         var cacheCountOnOutput: Option[Boolean]) {

  continueOnFailedStep = Some(continueOnFailedStep.getOrElse(false))
  cacheCountOnOutput = Some(cacheCountOnOutput.getOrElse(false))

  def getReaders: Seq[Reader] = inputs.getOrElse(Map()).map {
    case (name, input) => input.getReader(name)
  }.toSeq
}

object Configuration {
  def apply(configPath: String): Configuration = {
    val objectMapper = new ObjectMapper(new YAMLFactory())
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue(new File(configPath), classOf[Configuration])
  }
}