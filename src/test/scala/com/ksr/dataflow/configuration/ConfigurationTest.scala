package com.ksr.dataflow.configuration

import com.ksr.dataflow.configuration.job.Configuration
import org.scalatest.FlatSpec

class ConfigurationTest extends FlatSpec {

  val path: String = getClass.getResource("/config/sales.yaml").getPath

  "A yaml config file" should "generate a Configuration object" in {
    val configuration: Configuration = Configuration(path)
    assert(configuration.appName.get === "SalesApp")
    assert(configuration.output.get.file.get.dir === "target/test-classes/output/sales/")
  }
}
