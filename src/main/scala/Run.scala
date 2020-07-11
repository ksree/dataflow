import org.apache.log4j.LogManager

object Run extends App {
  val log = LogManager.getLogger(this.getClass)
  log.info("Starting Dataflow job")
}
