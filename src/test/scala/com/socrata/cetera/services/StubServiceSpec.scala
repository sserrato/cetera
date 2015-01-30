package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import org.scalatest.Matchers
import org.scalatest.WordSpec

class StubServiceSpec extends WordSpec with Matchers {
  "Stub object" should {
    "have a valid json stub with 2 results" in {
      val stub = Stub.getStub
      stub shouldBe a [JValue]
      stub.dyn.results(0).? should be ('right)
      stub.dyn.results(1).? should be ('right)
      stub.dyn.results(2).? should be ('left)
    }
  }
}
