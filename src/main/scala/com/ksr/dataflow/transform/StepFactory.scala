package com.ksr.dataflow.transform

import java.io.File

import com.ksr.dataflow.configuration.transform.Step
import com.ksr.dataflow.exceptions.DataFlowException
import com.ksr.dataflow.transform.stepActions.{Code, Sql}
import com.ksr.dataflow.utils.FileUtils


object StepFactory {
  def getStepAction(configuration: Step, metricDir: Option[File], metricName: String,
                    showPreviewLines: Int, cacheOnPreview: Option[Boolean],
                    showQuery: Option[Boolean]): StepAction[_] = {
    configuration.sql match {
      case Some(expression) => Sql(expression, configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery)
      case None => {
        configuration.file match {
          case Some(filePath) =>
            val path = metricDir match {
              case Some(dir) => new File(dir, filePath).getPath
              case _ => filePath
            }
            Sql(
              FileUtils.readConfigurationFile(path),
              configuration.dataFrameName, showPreviewLines, cacheOnPreview, showQuery
            )
          case None => {
            configuration.classpath match {
              case Some(cp) => {
                Code(cp, metricName, configuration.dataFrameName, configuration.params)
              }
              case None => throw DataFlowException("Each step requires an SQL query or a path to a file (SQL/Scala)")
            }
          }
        }
      }
    }
  }
}
