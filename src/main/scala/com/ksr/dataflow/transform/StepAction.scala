package com.ksr.dataflow.transform

import org.apache.spark.sql.SparkSession

trait StepAction[A] {
  def dataFrameName: String

  def run(sparkSession: SparkSession): A
}

