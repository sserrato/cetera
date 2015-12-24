package com.socrata.cetera.search

import java.io.Closeable
import java.net.InetSocketAddress

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.LoggerFactory

class ElasticSearchClient(host: String, port: Int, clusterName: String) extends Closeable {
  val logger = LoggerFactory.getLogger(getClass)

  val settings = Settings.settingsBuilder()
    .put("cluster.name", clusterName)
    .put("client.transport.sniff", true)
    .build

  val client: Client = TransportClient.builder()
    .settings(settings)
    .build
    .addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(host, port)))

  def close(): Unit = client.close()
}
