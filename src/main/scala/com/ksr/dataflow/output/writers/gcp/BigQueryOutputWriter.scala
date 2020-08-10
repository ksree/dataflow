package com.ksr.dataflow.output.writers.gcp

import com.ksr.dataflow.configuration.job.output.GCS
import com.ksr.dataflow.output.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

class BigQueryOutputWriter(props: Map[String, String], gcsConf: Option[GCS]) extends Writer{

  case class BigQueryOutputProperties(saveMode: SaveMode, dbTable: String)

  @transient lazy val log = LogManager.getLogger(this.getClass)
  val bqOptions = BigQueryOutputProperties(SaveMode.valueOf(props("saveMode")), props("dbTable"))

  override def write(dataFrame: DataFrame): Unit = {
    dataFrame.sparkSession.sparkContext.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    dataFrame.sparkSession.sparkContext.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    val writer: DataFrameWriter[Row] = dataFrame.write.format("bigquery")
    setCredentials(writer)
    setTemporaryBucket(writer)

    writer
      .format("bigquery")
      .mode(bqOptions.saveMode)
      .save(bqOptions.dbTable)
  }

  def setCredentials(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = gcsConf match {
    case Some(conf: GCS) => conf.credentialsFile match {
      case Some(cred) => writer.option("credentialsFile", cred)
      case None => writer
    }
    case _ => writer
  }

  def setTemporaryBucket(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = gcsConf match {
    case Some(conf: GCS) => conf.temporaryGcsBucket match {
      case Some(b) => writer.option("temporaryGcsBucket",b)
      case None => writer
    }
    case _ => writer
  }
}
