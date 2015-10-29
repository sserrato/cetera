package com.socrata.cetera.util

import org.scalatest.{Matchers, FunSuiteLike}

class ElasticsearchErrorSpec extends FunSuiteLike with Matchers {
  test("parse this one error") {
    val thisOneError = "Failed to execute phase [query], all shards failed; shardFailures {[NDfvHsbsQnid-YLZ9_nKGw][201510230201_calendars][0]: SearchParseException[[201510230201_calendars][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [Failed to parse source [{\\\"from\\\":0,\\\"size\\\":100,\\\"query\\\":{\\\"filtered\\\":{\\\"query\\\":{\\\"match_all\\\":{}},\\\"filter\\\":{\\\"and\\\":{\\\"filters\\\":[{\\\"terms\\\":{\\\"socrata_id.domain_cname.raw\\\":[\\\"data.cityofchicago.org\\\"]}},{\\\"not\\\":{\\\"filter\\\":{\\\"terms\\\":{\\\"moderation_status\\\":[\\\"pending\\\",\\\"rejected\\\"]}}}},{\\\"or\\\":{\\\"filters\\\":[]}}]}}}},\\\"sort\\\":[{\\\"page_views.page_views_total\\\":{\\\"order\\\":\\\"desc\\\"}}]}]]]; nested: SearchParseException[[201510230201_calendars][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [No mapping found for [page_views.page_views_total] in order to sort on]]; }{[NDfvHsbsQnid-YLZ9_nKGw][201510230201_charts][0]: SearchParseException[[201510230201_charts][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [Failed to parse source [{\\\"from\\\":0,\\\"size\\\":100,\\\"query\\\":{\\\"filtered\\\":{\\\"query\\\":{\\\"match_all\\\":{}},\\\"filter\\\":{\\\"and\\\":{\\\"filters\\\":[{\\\"terms\\\":{\\\"socrata_id.domain_cname.raw\\\":[\\\"data.cityofchicago.org\\\"]}},{\\\"not\\\":{\\\"filter\\\":{\\\"terms\\\":{\\\"moderation_status\\\":[\\\"pending\\\",\\\"rejected\\\"]}}}},{\\\"or\\\":{\\\"filters\\\":[]}}]}}}},\\\"sort\\\":[{\\\"page_views.page_views_total\\\":{\\\"order\\\":\\\"desc\\\"}}]}]]]; nested: SearchParseException[[201510230201_charts][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [No mapping found for [page_views.page_views_total] in order to sort on]]; }"
    val expectedShortMessage =
      """Failed to execute phase [query], all shards failed;
        |example shard failure:
        |{[NDfvHsbsQnid-YLZ9_nKGw][201510230201_calendars][0]: SearchParseException[[201510230201_calendars][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [Failed to parse source [{\"from\":0,\"size\":100,\"query\":{\"filtered\":{\"query\":{\"match_all\":{}},\"filter\":{\"and\":{\"filters\":[{\"terms\":{\"socrata_id.domain_cname.raw\":[\"data.cityofchicago.org\"]}},{\"not\":{\"filter\":{\"terms\":{\"moderation_status\":[\"pending\",\"rejected\"]}}}},{\"or\":{\"filters\":[]}}]}}}},\"sort\":[{\"page_views.page_views_total\":{\"order\":\"desc\"}}]}]]]; nested: SearchParseException[[201510230201_calendars][0]: query[ConstantScore(+cache(socrata_id.domain_cname.raw:data.cityofchicago.org) +NotFilter(cache(moderation_status:pending moderation_status:rejected)))],from[0],size[100]: Parse Failure [No mapping found for [page_views.page_views_total] in order to sort on]]; }""".stripMargin
    val expectedStackTrace = "ElasticsearchErrorSpec.test(ElasticsearchErrorSpec.scala:42)\n"

    val e = new Exception(thisOneError)
    e.setStackTrace(Array(
      new StackTraceElement("ElasticsearchErrorSpec", "test", "ElasticsearchErrorSpec.scala", 42)
    ))
    val eem = ElasticsearchError(e)

    eem.shortMessage should be(expectedShortMessage)
    eem.getStackTraceString should be(expectedStackTrace)
  }

  test("null error") {
    ElasticsearchError(null).shortMessage should be("")
  }

  test("empty error") {
    ElasticsearchError(new Exception("")).shortMessage should be("")
  }

  test("error doesn't contain shard failures") {
    val clusterError = """[2015-10-27 00:01:32,151][WARN ][discovery.zen.ping.unicast] [i-354208ee.cetera-search-server.us-west-2c.aws-us-west-2-staging.socrata.net] failed to send ping to [[#cloud-i-dbf5161f-0][ip-10-110-40-187.us-west-2.compute.internal][inet[/10.110.36.89:9300]]]
org.elasticsearch.transport.ReceiveTimeoutTransportException: [][inet[/10.110.36.89:9300]][internal:discovery/zen/unicast_gte_1_4] request_id [581320] timed out after [3750ms]"""
    ElasticsearchError(new Exception(clusterError)).shortMessage should be(clusterError)
  }
}
