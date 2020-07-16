package com.ksr.dataflow.output.writers.file

import com.ksr.dataflow.configuration.job.output.File

class JSONOutputWriter(props: Map[String, String], outputFile: Option[File])
  extends FileOutputWriter(Option(props).getOrElse(Map()) + ("format" -> "json"), outputFile)
