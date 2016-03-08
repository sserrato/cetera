package com.socrata.cetera

import java.io.File
import java.nio.file.Files

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._

import com.socrata.cetera.search.ElasticSearchClient

class TestESClient(testSuiteName: String)
  extends ElasticSearchClient("", 0, "", testSuiteName) { // host:port & cluster name are immaterial

  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = ImmutableSettings.settingsBuilder()
    .put("cluster.name", testSuiteName)
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
