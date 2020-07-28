package com.ksr.dataflow

import com.ksr.dataflow.configuration.job.Configuration
import com.ksr.dataflow.transform.Transformations
import org.apache.log4j.LogManager

object Run {
  val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]) {
    val configPath = args(0)
    log.info(s"Starting Dataflow job with configuration $configPath")
    val session = Job(Configuration(configPath))
    runTransformations(session)
  }

  def runTransformations(job: Job): Unit = {
    job.config.transformations match {
      case Some(t) => t.foreach(tPath => {
        val transformations = new Transformations(tPath)
        transformations.run(job)
      })
      case None => log.warn("No mertics were defined, exiting")
    }
  }

}
