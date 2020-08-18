package com.ksr.dataflow.input.readers.file

import com.ksr.dataflow.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FileInput(name: String,
                     paths: Seq[String],
                     options: Option[Map[String, String]],
                     schemaPath: Option[String],
                     format: Option[String]) extends Reader with FileInputBase {
  def read(sparkSession: SparkSession): DataFrame = {
    val readFormat = getFormat(format, paths.head)
    //Azure storage access info
    val blob_account_name = "pandemicdatalake"
    val blob_container_name = "public"
    val blob_relative_path = "curated/covid-19/ecdc_cases/latest/ecdc_cases.csv"
    val blob_sas_token = ""
    val wasbs_path = s"wasbs://$blob_container_name@$blob_account_name.blob.core.windows.net/$blob_relative_path"
    sparkSession.sparkContext.hadoopConfiguration.set(
      s"fs.azure.sas.$blob_container_name.$blob_account_name.blob.core.windows.net",
      blob_sas_token
    )
    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    val reader = sparkSession.read.format(readFormat)
    val readOptions = getOptions(readFormat, options)
    val schema = getSchemaStruct(schemaPath, sparkSession)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None =>
    }

    schema match {
      case Some(schemaStruct) => reader.schema(schemaStruct)
      case None =>
    }
    val df = reader.csv(wasbs_path)
    // val df = reader.load(paths: _*)

    processDF(df, readFormat)
  }
}
