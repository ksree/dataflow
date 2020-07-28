package com.ksr.dataflow.configuration.transform

case class Step(sql: Option[String],
                file: Option[String],
                classpath: Option[String],
                dataFrameName: String,
                params: Option[Map[String, String]],
                var ignoreOnFailures: Option[Boolean]) {

  ignoreOnFailures = Some(ignoreOnFailures.getOrElse(false))
}