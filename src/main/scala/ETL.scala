import org.apache.log4j.LogManager

object ETL extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Dataflow ETL job")
}
