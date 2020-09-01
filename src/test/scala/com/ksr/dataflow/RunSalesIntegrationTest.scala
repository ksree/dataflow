package com.ksr.dataflow

import com.ksr.dataflow.configuration.job.Configuration
import org.scalatest.{FlatSpec, Ignore}

class RunSalesIntegrationTest extends FlatSpec {

  val path: String = getClass.getResource("/config/sales.yaml").getPath
  val configuration: Configuration = Configuration(path)
  val session: Job = Job(Configuration(path), "test")
  val outputPath = "target/test-classes/output/sales/"
  val expectedSalesInUK = 100
  val expectedSalesInUS = 463

  "run" should "create a new test session" in {
    assert(session.env === "test")
    assert(session.config.appName.get === "TransactionsApp")
  }

  behavior of "runTransformations"
  Run.runTransformations(session)

  it should "run the transformations specified in the transformations.yaml and create salesInUK parquet file " in {
    val ukDF = session.sparkSession.read.format("parquet").load(s"$outputPath\\salesInUK.parquet")
    assert(ukDF.count === expectedSalesInUK)
  }

  it should "run the transformations specified in the transformations.yaml and create salesInUS parquet file " in {
    val usDF = session.sparkSession.read.format("parquet").load(s"$outputPath\\salesInUS.parquet")
    assert(usDF.count === expectedSalesInUS)
  }

}
