package com.ksr.dataflow.output

import com.ksr.dataflow.Job
import com.ksr.dataflow.configuration.job.Configuration
import com.ksr.dataflow.configuration.transform.{Output, OutputType}
import com.ksr.dataflow.exceptions.DataFlowException
import com.ksr.dataflow.output.writers.file.{CSVOutputWriter, FileOutputWriter, JSONOutputWriter, ParquetOutputWriter}
import com.ksr.dataflow.output.writers.gcp.BigQueryOutputWriter
import com.ksr.dataflow.output.writers.jdbc.JDBCOutputWriter


object WriterFactory {
  def get(outputConfig: Output, metricName: String, configuration: Configuration, job: Job): Writer = {
    val output = outputConfig.name match {
      case Some(name) => configuration.outputs.get.get(name).get
      case None => configuration.output.getOrElse(com.ksr.dataflow.configuration.job.Output())
    }
    val transformationOutputOptions = outputConfig.outputOptions.asInstanceOf[Map[String, String]]

    val transformationOutputWriter = outputConfig.outputType match {
      case OutputType.File => new FileOutputWriter(transformationOutputOptions, output.file)
      case OutputType.CSV => new CSVOutputWriter(transformationOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(transformationOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(transformationOutputOptions, output.file)
      case OutputType.JDBC => new JDBCOutputWriter(transformationOutputOptions, output.jdbc)
      case OutputType.AzureSQL => new JDBCOutputWriter(transformationOutputOptions, output.azure)
      case OutputType.AWSRedshift => new JDBCOutputWriter(transformationOutputOptions, output.aws)
      case OutputType.GCPBigQuery => new BigQueryOutputWriter(transformationOutputOptions, output.gcp)
      case _ => throw new DataFlowException(s"Not Supported Writer ${outputConfig.outputType}")
    }
    transformationOutputWriter.validateMandatoryArguments(transformationOutputOptions.asInstanceOf[Map[String, String]])
    transformationOutputWriter
  }
}
