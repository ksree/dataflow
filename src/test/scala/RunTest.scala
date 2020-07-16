import com.ksr.dataflow.{Job, Run}
import com.ksr.dataflow.configuration.job.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, FunSuite}

class RunTest extends FlatSpec {

  val path: String = getClass.getResource("/config/sales.yaml").getPath
  val configuration: Configuration = Configuration(path)
  val session = Job(Configuration(path), "test")

  "run" should "create a new test session" in {
    assert(session.env === "test")
    assert(session.config.appName.get === "TransactionsApp")
  }

  "runTransformations" should "run the transformations specified in the transformations.yaml" in {
    Run.runTransformations(session)
  }

}
