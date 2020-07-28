package com.ksr.dataflow.configuration.job.output

case class Kafka(config: String,
                 checkpointLocation: Option[String],
                 compressionType: Option[String]
                ) {
  require(Option(config).isDefined, "Kafka connection: servers are mandatory.")
}
