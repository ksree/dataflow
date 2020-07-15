package com.ksr.dataflow.configuration.job.input

import com.ksr.dataflow.input.Reader

//TODO :  Provide implementation
case class Kafka() extends InputConfig {
  override def getReader(name: String): Reader = ???
}
