package com.socrata.cetera.util

case class ElasticsearchError(originalMessage: String, stackTrace: Array[StackTraceElement]) extends Throwable {
  override def getStackTrace: Array[StackTraceElement] = stackTrace
  override def toString: String = shortMessage

  lazy val shortMessage: String = originalMessage match {
    case s: String if s.length < 1024 => s
    case _ =>
      val splits = splitOption(originalMessage, "shardFailures", 2)
      val primaryMessage = splits(0).map(_.trim)
      val exampleShardMessage = splits(1).flatMap { s =>
        s.replace("}{", "}\n{").split("\n", 2).headOption.map { sx =>
          s"""
              |example shard failure:
              |${sx.trim}""".stripMargin
        }
      }
      primaryMessage.getOrElse("") + exampleShardMessage.getOrElse("")
  }

  private def splitOption(string: String, regex: String, limit: Int): Seq[Option[String]] = {
    val is = Seq.range(0, limit)
    val ss = string.split(regex, limit)
    is.map { i => if (ss.isDefinedAt(i)) Option(ss(i)) else None }
  }
}

object ElasticsearchError {
  def apply(e: Throwable): ElasticsearchError = {
    ElasticsearchError(e.getMessage, e.getStackTrace)
  }
}
