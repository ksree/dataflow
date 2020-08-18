package com.ksr.dataflow.configuration.transform

import java.io.{File, FileNotFoundException}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.ksr.dataflow.exceptions.DataFlowInvalidFileException
import com.ksr.dataflow.transform.Transformation
import com.ksr.dataflow.utils.FileUtils
import org.apache.commons.io.FilenameUtils
import org.apache.log4j.{LogManager, Logger}

case class Configuration(steps: List[Step], output: Option[List[Output]])

object Configuration {
  val log: Logger = LogManager.getLogger(this.getClass)

  val validExtensions = Seq("json", "yaml", "yml")

  def isValidFile(path: File): Boolean = {
    val fileName = path.getName
    val extension = FilenameUtils.getExtension(fileName)
    validExtensions.contains(extension)
  }

  def parse(path: String): Transformation = {
/*    val hadoopPath = FileUtils.getHadoopPath(path)
    val fileName = hadoopPath.getName
    val metricDir = FileUtils.isLocalFile(path) match {
      case true => Option(new File(path).getParentFile)
      case false => None
    }*/
    log.info(s"path = $path")

    val fileName = getClass.getClassLoader.getResource(path).getPath
    log.info(s"fileName = $fileName")
    val metricDir: Option[File] = Some(new File(fileName).getParentFile)
    log.info(s"Initializing transformation file $fileName")
    try {
      val metricConfig: Configuration = parseFile(fileName)
      Transformation(metricConfig, metricDir, FilenameUtils.removeExtension(path))
    } catch {
      case e: FileNotFoundException => throw e
      case e: Exception => throw DataFlowInvalidFileException(s"Failed to parse metric file $path", e)
    }
  }

  private def parseFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(new File(fileName), classOf[Configuration])
      }
      case None => throw DataFlowInvalidFileException(s"Unknown extension for file $fileName")
    }
  }
}