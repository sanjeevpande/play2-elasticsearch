import com.github.cleverage.elasticsearch.{IndexService, IndexClient}
import org.specs2.mutable.Specification
import play.api.test.Helpers._

/**
 * Base tests for the Elasticsearch Plugin
 */
class ElasticsearchPluginSpec extends Specification with ElasticsearchTestHelper {

  sequential

  "ElasticsearchPlugin" should {
    "provide an elasticsearch client on start" in {
      running(esFakeApp) {
        IndexClient.client must not beNull
      }
    }
    "provide an elasticsearch node on start" in {
      running(esFakeApp) {
        IndexClient.node must not beNull
      }
    }
    "create the index on start" in {
      running(esFakeApp) {
        IndexService.existsIndex must beTrue
      }
    }
    "allow deleting an index" in {
      running(esFakeApp) {
        IndexService.deleteIndex
        IndexService.existsIndex must beFalse
      }
    }
  }

}