package com.ksr.dataflow.configuration.job

import com.ksr.dataflow.configuration.job.output.{File, JDBC, Kafka}

case class Output(jdbc: Option[JDBC] = None,
                  file: Option[File] = None,
                  kafka: Option[Kafka] = None)
