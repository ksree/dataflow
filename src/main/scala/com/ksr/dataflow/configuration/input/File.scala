package com.ksr.dataflow.configuration.input

import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.file.FileInput

case class File(path: String,
                options: Option[Map[String, String]],
                schemaPath: Option[String],
                format: Option[String],
                isStream: Option[Boolean]) extends InputConfig {
  override def getReader(name: String): Reader = {
    FileInput(name, path, options, schemaPath, format)
  }
}

trait InputConfig {
  def getReader(name: String): Reader
}
