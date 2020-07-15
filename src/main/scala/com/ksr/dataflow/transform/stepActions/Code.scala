package com.ksr.dataflow.transform.stepActions

import com.ksr.dataflow.transform.StepAction
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._

case class Code(objectClassPath: String, transformationName: String, dataFrameName: String, params: Option[Map[String, String]]) extends StepAction[Unit] {
  type DataflowCustomCode = {
    def run(sparkSession: SparkSession, transform: String, step: String, params: Option[Map[String, String]]): Unit
  }

  val rm = runtimeMirror(getClass.getClassLoader)
  val module = rm.staticModule(objectClassPath)

  val obj = rm.reflectModule(module).instance.asInstanceOf[DataflowCustomCode]

  override def run(sparkSession: SparkSession): Unit = {
    obj.run(sparkSession, transformationName, dataFrameName, params)
  }
}
