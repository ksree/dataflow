import com.ksr.dataflow.configuration.Configuration
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class Job(config: Configuration) {
  private val log = LogManager.getLogger(this.getClass)
  val sparkSession: SparkSession = createSparkSession(config.appName)
  val sparkContext: SparkContext = sparkSession.sparkContext

  private def createSparkSession(appName: Option[String]): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder().appName(appName.get)
    sparkSessionBuilder.getOrCreate()
  }

  setSparkLogLevel(config.logLevel, sparkContext)

  private def setSparkLogLevel(logLevel: Option[String], sparkContext: SparkContext) {
    logLevel match {
      case Some(level) => sparkContext.setLogLevel(level)
      case None =>
    }
  }
}