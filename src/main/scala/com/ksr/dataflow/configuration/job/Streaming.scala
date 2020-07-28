package com.ksr.dataflow.configuration.job

import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

case class Streaming(triggerMode: Option[String],
                     triggerDuration: Option[String],
                     outputMode: Option[String],
                     checkpointLocation: Option[String],
                     batchMode: Option[Boolean],
                     format: Option[String],
                     path: Option[String],
                     extraOptions: Option[Map[String, String]]) {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  def applyOptions(writer: DataStreamWriter[_]): Unit = {
    checkpointLocation match {
      case Some(location) => writer.option("checkpointLocation", location)
      case None =>
    }

    outputMode match {
      case Some(outputMode) => writer.outputMode(outputMode)
      case None =>
    }

    format match {
      case Some(format) => writer.format(format)
      case None =>
    }

    path match {
      case Some(path) => writer.option("path", path)
      case None =>
    }

    (triggerMode, triggerDuration) match {
      case (Some("ProcessingTime"), Some(duration)) =>
        writer.trigger(Trigger.ProcessingTime(duration))
      case (Some("Once"), _) =>
        writer.trigger(Trigger.Once())
      case (Some("Continuous"), Some(duration)) =>
        writer.trigger(Trigger.Continuous(duration))
      case _ =>
        log.warn("no triggerMode was passed or trigger sent is invalid. writer will be returned with default trigger mode")
        writer
    }

    extraOptions match {
      case Some(options) => writer.options(options)
      case None =>
    }
  }
}
