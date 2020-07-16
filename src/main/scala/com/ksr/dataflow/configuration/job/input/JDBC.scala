package com.ksr.dataflow.configuration.job.input

import com.ksr.dataflow.configuration.job.InputConfig
import com.ksr.dataflow.input.Reader

//TODO :  Provide implementation
case class JDBC() extends InputConfig {
  override def getReader(name: String): Reader = ???
}

