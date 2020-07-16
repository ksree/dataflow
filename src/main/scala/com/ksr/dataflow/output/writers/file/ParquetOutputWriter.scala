package com.ksr.dataflow.output.writers.file

import com.ksr.dataflow.configuration.job.output.File

class ParquetOutputWriter(props: Map[String, String], outputFile: Option[File])
  extends FileOutputWriter(Option(props).getOrElse(Map()) + ("format" -> "parquet"), outputFile)

