package com.ksr.dataflow.input

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Reader {
  val name: String

  def read(sparkSession: SparkSession): DataFrame
}
