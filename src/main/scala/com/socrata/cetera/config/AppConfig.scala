package com.socrata.cetera.config

import com.socrata.thirdparty.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

/**
 * Contains configuration values from the application config file
 * @param config Configuration object
 */
class CeteraConfig(config: Config) extends ConfigClass(config, "com.socrata") {
  val server = getConfig("cetera", new ServerConfig(_, _))
  val log4j = getRawConfig("log4j")
  val http = getConfig("http", new HttpConfig(_, _))
  val elasticSearch = getConfig("elasticsearch", new ElasticSearchConfig(_,_))

  val debugString = config.root.render()
}

class HttpConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val liveness = getConfig("liveness", new LivenessConfig(_, _))
}

class LivenessConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val interval = getDuration("interval")
  val range = getDuration("range")
  val missable = getInt("missable")
}

class ServerConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
  val gracefulShutdownTimeout = getDuration("graceful-shutdown-time")
}

class ElasticSearchConfig(config:Config, root:String) extends ConfigClass(config,root) {
  protected def getBoostMap(config: Config, key: String): Float =
    config.getNumber(key).floatValue

  val elasticSearchServer = getString("es-server")
  val elasticSearchPort = getInt("es-port")
  val elasticSearchClusterName = getString("es-cluster-name")
  val titleBoost = optionally[Float](config.getDouble(path("title-boost")).toFloat)
  val minShouldMatch = optionally[String](getString("min-should-match"))
  val functionScoreScripts = getStringList("function-score-scripts")
  val typeBoosts = getObjectOf[Float]("type-boosts", getBoostMap _)
}
