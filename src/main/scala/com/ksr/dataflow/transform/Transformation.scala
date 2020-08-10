package com.ksr.dataflow.transform

import java.io.File

import com.ksr.dataflow.Job
import com.ksr.dataflow.configuration.job.Streaming
import com.ksr.dataflow.configuration.transform.{Configuration, Output}
import com.ksr.dataflow.exceptions.{DataFlowFailedStepException, DataFlowWriteFailedException}
import com.ksr.dataflow.output.{Writer, WriterFactory}
import org.apache.log4j.LogManager
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.{DataFrame, Dataset}

case class Transformation(configuration: Configuration, transformationDir: Option[File], transformationName: String) {
  val log = LogManager.getLogger(this.getClass)

  def calculate(job: Job): Unit = {
    val tags = Map("transformation" -> transformationName)
    for (stepConfig <- configuration.steps) {
      val step = StepFactory.getStepAction(stepConfig, transformationDir, transformationName, job.config.showPreviewLines.getOrElse(0),
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

  private def writeStream(dataFrame: DataFrame,
                          dataFrameName: String,
                          streamingConfig: Option[Streaming],
                          writer: Writer,
                          outputConfig: Output): Unit = {
    log.info(s"Starting to write streaming results of ${dataFrameName}")
    streamingConfig match {
      case Some(config) => {
        val streamWriter = dataFrame.writeStream
        config.applyOptions(streamWriter)
        config.batchMode match {
          case Some(true) => {
            val query = streamWriter.foreachBatch((batchDF: DataFrame, _: Long) => {
              log.info(s"inside foreach batch of $dataFrameName")
              batchDF.show(10)
              writer.write(batchDF)
            }).start()
           dataFrame.sparkSession.streams.awaitAnyTermination()
            // Exit this function after streaming is completed
            return
          }
          case _ =>
        }
      }
      case None =>
    }
    // Non batch mode
    /*    writerConfig.writers.size match {
          case size if size == 1 => writerConfig.writers.foreach(writer => writer.writeStream(writerConfig.dataFrame, streamingConfig))
          case size if size > 1 => log.error("Found multiple outputs for a streaming source without using the batch mode, " +
            "skipping streaming writing")
          case _ =>
        }*/
  }


  def write(job: Job): Unit = {
    val a: Dataset[Table] = job.sparkSession.catalog.listTables()
    val b = a.collect()
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
          if (dataFrame.isStreaming) {
            writeStream(dataFrame, dataFrameName, job.config.streaming, writer, outputConfig)
          } else {
            writeBatch(dataFrame, dataFrameName, writer, outputConfig, job.config.cacheCountOnOutput)
          }
        })

      }
      case None =>
    }
  }
}
