package com.socrata.cetera.metrics

import java.io.File

import com.blist.metrics.impl.queue.MetricFileQueue
import com.socrata.metrics.{QueryString, DomainId}
import org.slf4j.LoggerFactory

class BalboaClient(dataDirectory: String) {

  private val queue = MetricFileQueue.getInstance(new File(dataDirectory))
  lazy val logger = LoggerFactory.getLogger(classOf[BalboaClient])

  def logQuery(domainId: Int, query: String): Unit = {
    logger info s"Logging query '$query' to balboa"
    queue.logDatasetSearch(DomainId(domainId), QueryString(query))
  }
}
