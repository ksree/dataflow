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
    val metricOutputOptions = outputConfig.outputOptions.asInstanceOf[Map[String, String]]

    val metricOutputWriter = outputConfig.outputType match {
      case OutputType.File => new FileOutputWriter(metricOutputOptions, output.file)
      case OutputType.CSV => new CSVOutputWriter(metricOutputOptions, output.file)
      case OutputType.JSON => new JSONOutputWriter(metricOutputOptions, output.file)
      case OutputType.Parquet => new ParquetOutputWriter(metricOutputOptions, output.file)
      case OutputType.JDBC => new JDBCOutputWriter(metricOutputOptions, output.jdbc)
      case OutputType.GCPBigQuery => new BigQueryOutputWriter(metricOutputOptions, output.gcp)
      case _ => throw new DataFlowException(s"Not Supported Writer ${outputConfig.outputType}")
    }
    metricOutputWriter.validateMandatoryArguments(metricOutputOptions.asInstanceOf[Map[String, String]])
    metricOutputWriter
  }
}
