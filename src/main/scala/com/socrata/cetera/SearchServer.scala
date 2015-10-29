package com.socrata.cetera

import java.util.concurrent.{ExecutorService, Executors}

import com.rojoma.simplearm.v2._
import com.socrata.cetera.config.CeteraConfig
import com.socrata.cetera.handlers.Router
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.services._
import com.socrata.cetera.types.ScriptScoreFunction
import com.socrata.http.client.InetLivenessChecker
import com.socrata.http.server._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.LoggerFactory

// $COVERAGE-OFF$ jetty wiring
object SearchServer extends App {
  val config = new CeteraConfig(ConfigFactory.load())
  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("Starting Cetera server on port {}... ", config.server.port)
  logger.info("Configuration:\n" + config.debugString)

  implicit def executorResource[A <: ExecutorService]: Resource[A] = new Resource[A] {
    def close(a: A): Unit = {
      a.shutdown()
    }
  }

  trait Startable { def start(): Unit }
  def managedStartable[T <: Startable : Resource](resource: => T): Managed[T] = new Managed[T] {
    override def run[A](f: T => A): A =
      using(resource) { r =>
        r.start()
        f(r)
      }
  }

  for {
    executor <- managed(Executors.newCachedThreadPool())

    livenessChecker <- managedStartable(
      new InetLivenessChecker(config.http.liveness.interval,
                              config.http.liveness.range,
                              config.http.liveness.missable,
                              executor) with Startable)

    elasticSearch <- managed(
      new ElasticSearchClient(config.elasticSearch.elasticSearchServer,
                              config.elasticSearch.elasticSearchPort,
                              config.elasticSearch.elasticSearchClusterName,
                              config.elasticSearch.typeBoosts,
                              config.elasticSearch.titleBoost,
                              config.elasticSearch.minShouldMatch,
                              config.elasticSearch.functionScoreScripts.flatMap(fnName =>
                                ScriptScoreFunction.getScriptFunction(fnName)).toSet))
  } {
    logger.info("ElasticSearchClient initialized on nodes " +
                  elasticSearch.client.asInstanceOf[TransportClient].transportAddresses().toString)

    logger.info("Initializing VersionService")
    val versionService = VersionService

    logger.info("Initializing SearchService with Elasticsearch TransportClient")
    val searchService = new SearchService(Some(elasticSearch))

    logger.info("Initializing FacetService with Elasticsearch TransportClient")
    val facetService = new FacetService(Some(elasticSearch))

    logger.info("Initializing FacetValueService with Elasticsearch TransportClient")
    val facetValueService = new FacetValueService(Some(elasticSearch))

    logger.info("Initializing CountService with Elasticsearch TransportClient")
    val countService = new CountService(Some(elasticSearch))

    logger.info("Initializing router with services")
    val router = new Router(
      versionService.Service,
      searchService.Service,
      facetService.aggregate,
      facetValueService.listValues,
      countService.Service
    )

    logger.info("Initializing handler")
    val handler = router.route _

    logger.info("Initializing server")
    val server = new SocrataServerJetty(
      handler,
      SocrataServerJetty
        .defaultOptions
        .withPort(config.server.port)
        .withGracefulShutdownTimeout(config.server.gracefulShutdownTimeout))

    logger.info("Running server!")
    server.run()
  }

  logger.info("All done!")
}
// $COVERAGE-ON$
