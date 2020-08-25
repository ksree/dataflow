package com.ksr.dataflow

import com.ksr.dataflow.configuration.job.Configuration
import org.scalatest.{FlatSpec, Ignore}

@Ignore
class RunKafkaIntegrationTest extends FlatSpec {

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
}
