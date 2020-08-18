package com.ksr.dataflow.input.readers.azure

import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.readers.file.FileInputBase
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AzureBlobStorageInput(name: String,
                                 containerName: String,
                                 storageAccountName: String,
                                 blob_sas_token: String,
                                 blob_relative_path: String,
                                 options: Option[Map[String, String]],
                                 format: Option[String]) extends Reader with FileInputBase {

  require(Option(blob_sas_token).isDefined, "SAS token or Account access key is requried")

  override def read(sparkSession: SparkSession): DataFrame = {
    val blob_account_name = "pandemicdatalake"
    val blob_container_name = "public"
    val blob_relative_path = "curated/covid-19/ecdc_cases/latest/ecdc_cases.csv"
    val blob_sas_token = ""
    val wasbs_path = s"wasbs://$blob_container_name@$blob_account_name.blob.core.windows.net/$blob_relative_path"
    val readFormat = getFormat(format, blob_relative_path.toString.split("\\.").last)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    sparkSession.sparkContext.hadoopConfiguration.set(
      s"fs.azure.sas.$blob_container_name.$blob_account_name.blob.core.windows.net", blob_sas_token)

    val reader = sparkSession.read.format(readFormat)
    val readOptions = getOptions(readFormat, options)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None =>
    }
    val df = reader.load(wasbs_path)
    processDF(df, readFormat)

  }
}
