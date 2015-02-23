package com.socrata.cetera.search

import java.io.Closeable

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

class ElasticSearchClient(host: String, port: Int, clusterName: String) extends Closeable {
  val settings = ImmutableSettings.settingsBuilder().
                   put("cluster.name", clusterName).
                   put("client.transport.sniff", true).
                   build()

  val client = new TransportClient(settings).
                 addTransportAddress(new InetSocketTransportAddress(host, port))

  def close(): Unit = client.close()
}
