package com.ksr.dataflow

import java.io.File

import com.ksr.dataflow.configuration.job.Configuration
import org.apache.spark.sql.streaming.DataStreamWriter
import org.scalatest.FlatSpec

class RunKafkaTest extends FlatSpec {

  val path: String = getClass.getResource("/config/kafka.yaml").getPath
  val configuration: Configuration = Configuration(path)
  val session = Job(Configuration(path), "test")

  "run" should "create a new test session" in {
    assert(session.env === "test")
    assert(session.config.appName.get === "KafkaApp")
  }

  "runTransformations" should "run the transformations specified in the transformations.yaml" in {
    Run.runTransformations(session)
  }
 "test file name" should "make sence" in {
   val u  = getClass.getResource("/config/covid_tracking_transformations.yaml")
   println(s"path = ${u.getPath}")
   val f = new File(u.getPath)
   f.getName
   println(s"file  name = ${f.getName}")
   println(s"file dir = ${f.getParentFile.getName}")

 }
}
