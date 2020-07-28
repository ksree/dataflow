package com.ksr.dataflow.configuration.job

import com.ksr.dataflow.configuration.job.input.{File, JDBC, Kafka}
import com.ksr.dataflow.input.Reader

case class Input(file: Option[File], jdbc: Option[JDBC], kafka: Option[Kafka]) extends InputConfig {
  def getReader(name: String): Reader = {
    Seq(file, jdbc, kafka).find(
      x => x.isDefined
    ).get.get.getReader(name)
  }
}

trait InputConfig {
  def getReader(name: String): Reader
}
