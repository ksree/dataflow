package com.ksr.dataflow.configuration.job.input

import com.ksr.dataflow.input.Reader
import com.ksr.dataflow.input.file.FileInput
import com.ksr.dataflow.utils.FileUtils

case class File(path: String,
                options: Option[Map[String, String]],
                schemaPath: Option[String],
                format: Option[String]) extends InputConfig {
  override def getReader(name: String): Reader = {
    FileUtils.getListOfLocalFiles(path)

    FileInput(name, path.split(","), options, schemaPath, format)
  }
}

