package com.ksr.dataflow.configuration.input

import com.ksr.dataflow.configuration.InputConfig
import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.file.FileInput

case class File(path: String,
                options: Option[Map[String, String]],
                schemaPath: Option[String],
                format: Option[String]) extends InputConfig {
  override def getReader(name: String): Reader = {
    FileInput(name, path.split(","), options, schemaPath, format)
  }
}

