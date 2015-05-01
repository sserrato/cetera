package com.socrata.cetera

import java.util.concurrent.{Executors, ExecutorService}
import scala.concurrent.duration._

import com.rojoma.simplearm.v2._
import com.socrata.http.client.InetLivenessChecker
import com.socrata.http.server._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.LoggerFactory

import com.socrata.cetera.config.CeteraConfig
import com.socrata.cetera.handlers.Router
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.services._

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

  def managedStartable[T <: { def start() } : Resource](resource: => T): Managed[T] = new Managed[T] {
    override def run[A](f: T => A): A =
      using(resource) { r =>
        import scala.language.reflectiveCalls
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
                              executor))

    elasticSearch <- managed(
      new ElasticSearchClient(config.elasticSearch.elasticSearchServer,
                              config.elasticSearch.elasticSearchPort,
                              config.elasticSearch.elasticSearchClusterName,
                              config.elasticSearch.useCustomRanker))
  } {
    logger.info("ElasticSearchClient initialized on nodes " +
                  elasticSearch.client.asInstanceOf[TransportClient].transportAddresses().toString +
                " using ranker " +
                {if(config.elasticSearch.useCustomRanker) "custom" else "default"})

    logger.info("Initializing VersionService")
    val versionService = VersionService

    logger.info("Initializing SearchService with Elasticsearch TransportClient")
    val searchService = new SearchService(elasticSearch)

    logger.info("Initializing CountService with Elasticsearch TransportClient")
    val countService = new CountService(elasticSearch)

    logger.info("Initializing router with services")
    val router = new Router(
      versionService.Service,
      searchService.Service,
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
