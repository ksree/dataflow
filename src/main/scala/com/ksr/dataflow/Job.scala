package com.ksr.dataflow

import com.ksr.dataflow.configuration.job.Configuration
import com.ksr.dataflow.input.Reader
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class Job(config: Configuration, env: String = "") {
  private val log = LogManager.getLogger(this.getClass)
  val sparkSession: SparkSession = createSparkSession(config.appName)

  val sparkContext: SparkContext = sparkSession.sparkContext

  log.info(s"these are the config inputs: ${config.inputs}")
  registerDataframes(config.getReaders, sparkSession)

  private def createSparkSession(appName: Option[String]): SparkSession = {
    val sparkSessionBuilder: SparkSession.Builder =
      env match {
        case "test" => SparkSession.builder().appName(appName.get).master("local")
        case _ => SparkSession.builder().appName(appName.get)
      }
    sparkSessionBuilder.getOrCreate()
  }

  setSparkLogLevel(config.logLevel, sparkContext)

  private def setSparkLogLevel(logLevel: Option[String], sparkContext: SparkContext) {
    logLevel match {
      case Some(level) => sparkContext.setLogLevel(level)
      case None =>
    }
  }

  def registerDataframes(inputs: Seq[Reader], sparkSession: SparkSession): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(input => {
        log.info(s"Registering ${input.name} table")
        val df = input.read(sparkSession)
        config.showPreviewLines match {
          case Some(x) => df.show(x)
          case _ =>
        }
        df.createOrReplaceTempView(input.name)
      })
    }
  }
}
