package com.ksr.dataflow.configuration.job.output

case class Kafka(servers: Seq[String],
                 checkpointLocation: Option[String],
                 compressionType: Option[String]
                ) {
  require(Option(servers).isDefined, "Kafka connection: servers are mandatory.")
}
