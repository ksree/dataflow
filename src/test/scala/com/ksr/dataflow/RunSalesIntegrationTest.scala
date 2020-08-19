package com.ksr.dataflow

import com.ksr.dataflow.configuration.job.Configuration
import org.scalatest.FlatSpec

class RunSalesIntegrationTest extends FlatSpec {

  val path: String = getClass.getResource("/config/sales.yaml").getPath
  val configuration: Configuration = Configuration(path)
  val session = Job(Configuration(path), "test")

  "run" should "create a new test session" in {
    assert(session.env === "test")
    assert(session.config.appName.get === "TransactionsApp")
  }

  "runTransformations" should "run the transformations specified in the transformations.yaml" in {
    Run.runTransformations(session)
  }

}