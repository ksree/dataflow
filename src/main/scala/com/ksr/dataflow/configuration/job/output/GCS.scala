package com.ksr.dataflow.configuration.job.output

case class GCS(credentialsFile: Option[String],
               temporaryGcsBucket: Option[String])
