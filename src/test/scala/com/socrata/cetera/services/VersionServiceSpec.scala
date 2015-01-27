package com.socrata.cetera.services

import org.scalatest.ShouldMatchers
import org.scalatest.WordSpec

class VersionServiceSpec extends WordSpec with ShouldMatchers {
  "Version resource" should {
    "have a working resource" in {
      val resource = Version.getVersion
      resource match {
        case Left(e) =>
          fail(e.toString)
        case Right(thing) =>
          thing.service should be ("cetera")
          // Heads up, regex! This does NOT capture all valid versions (e.g., 1.2beta1)
          thing.version should fullyMatch regex ("""\d+(\.\d+){0,2}(-SNAPSHOT)""")
          thing.scalaVersion should be ("2.10.4")
      }
    }
  }
}
