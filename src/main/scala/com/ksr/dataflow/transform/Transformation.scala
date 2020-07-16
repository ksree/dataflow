package com.ksr.dataflow.transform

import java.io.File

import com.ksr.dataflow.Job
import com.ksr.dataflow.configuration.transform.{Configuration, Output}
import com.ksr.dataflow.exceptions.{DataFlowFailedStepException, DataFlowWriteFailedException}
import com.ksr.dataflow.output.{Writer, WriterFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

case class Transformation(configuration: Configuration, transformationDir: Option[File], transformationName: String) {
  val log = LogManager.getLogger(this.getClass)

  def calculate(job: Job): Unit = {
    val tags = Map("transformation" -> transformationName)
    for (stepConfig <- configuration.steps) {
      val step = StepFactory.getStepAction(stepConfig, transformationDir, transformationName, job.config.showPreviewLines.get,
        job.config.cacheOnPreview, job.config.showQuery)
      try {
        log.info(s"Calculating step ${step.dataFrameName}")
        step.run(job.sparkSession)
      } catch {
        case ex: Exception => {
          val errorMessage = s"Failed to calculate dataFrame: ${step.dataFrameName} on transformation: ${transformationName}"
          if (stepConfig.ignoreOnFailures.get || job.config.continueOnFailedStep.get) {
            log.error(errorMessage + " - " + ex.getMessage)
          } else {
            throw DataFlowFailedStepException(errorMessage, ex)
          }
        }
      }
    }
  }

  private def writeBatch(dataFrame: DataFrame,
                         dataFrameName: String,
                         writer: Writer,
                         outputConfig: Output,
                         cacheCountOnOutput: Option[Boolean]): Unit = {

    val dataFrameCount = cacheCountOnOutput match {
      case Some(true) => {
        dataFrame.cache()
        dataFrame.count()
      }
      case _ => 0
    }
    val tags = Map("metric" -> transformationName, "dataframe" -> dataFrameName, "output_type" -> outputConfig.outputType.toString)
    log.info(s"Starting to Write results of ${dataFrameName}")
    try {
      writer.write(dataFrame)
    } catch {
      case ex: Exception => {
        throw DataFlowWriteFailedException(s"Failed to write dataFrame: " +
          s"$dataFrameName to output: ${outputConfig.outputType} on metric: ${transformationName}", ex)
      }
    }
  }

  private def repartition(outputConfig: Output, dataFrame: DataFrame): DataFrame = {
    // Backward compatibility
    val deprecatedRepartition = Option(outputConfig.outputOptions).getOrElse(Map()).get("repartition").asInstanceOf[Option[Int]]
    val deprecatedCoalesce = Option(outputConfig.outputOptions).getOrElse(Map()).get("coalesce").asInstanceOf[Option[Boolean]]

    (outputConfig.coalesce.orElse(deprecatedCoalesce),
      outputConfig.repartition.orElse(deprecatedRepartition)) match {
      case (Some(true), _) => dataFrame.coalesce(1)
      case (_, Some(repartition)) => dataFrame.repartition(repartition)
      case _ => dataFrame
    }
  }


  def write(job: Job): Unit = {

    configuration.output match {
      case Some(output) => {
        output.foreach(outputConfig => {
          val writer = WriterFactory.get(outputConfig, transformationName, job.config, job)
          val dataFrameName = outputConfig.dataFrameName
          val dataFrame = repartition(outputConfig, job.sparkSession.table(dataFrameName))


          val outputOptions = Option(outputConfig.outputOptions).getOrElse(Map())
          outputOptions.get("protectFromEmptyOutput").asInstanceOf[Option[Boolean]] match {
            case Some(true) => {
              if (dataFrame.head(1).isEmpty) {
                throw DataFlowWriteFailedException(s"Abort writing dataframe: ${dataFrameName}, " +
                  s"empty dataframe output is not allowed according to configuration")
              }
            }
            case _ =>
          }

          writeBatch(dataFrame, dataFrameName, writer, outputConfig, job.config.cacheCountOnOutput)
        })

      }
      case None =>
    }
  }
}
