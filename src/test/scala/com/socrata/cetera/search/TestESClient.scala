package com.socrata.cetera.search

import java.io.File
import java.nio.file.Files

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._

class TestESClient(val clusterName: String, val datatypeBoosts: Map[String, Float] = Map.empty)
  extends ElasticSearchClient("local", 0, "useless", datatypeBoosts, None, None, Set.empty) {
  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = ImmutableSettings.settingsBuilder()
    .put("cluster.name", clusterName)
    .put("client.transport.sniff", false)
    .put("discovery.zen.ping.multicast.enabled", false)
    .put("path.data", tempDataDir.toString)
    .build
  val node = nodeBuilder().settings(testSettings).local(true).node()

  override val client = node.client()
  override def close(): Unit = {
    node.close()

    try { // don't care if cleanup succeeded or failed
      def deleteR(file: File): Unit = {
        if (file.isDirectory) Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteR)
        file.delete()
      }
      deleteR(tempDataDir)
    } catch { case e: Exception => () }

    super.close()
  }
}
