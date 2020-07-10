package com.ksr.dataflow.configuration

import org.scalatest.FlatSpec

class ConfigurationTest extends FlatSpec {

  val path: String = getClass.getResource("/movies.yaml").getPath

  "A yaml config file" should "generate a Configuration object" in {
    val configuration: Configuration = Configuration(path)

  }
}
