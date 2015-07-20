package com.socrata.cetera.services

import org.scalatest.ShouldMatchers
import org.scalatest.WordSpec

class VersionServiceSpec extends WordSpec with ShouldMatchers {
  "Version resource" should {
    "get version via BuildInfoPlugin" in {
      val resource = VersionService.version
          resource.name should be ("cetera")
          // Heads up, regex! This does NOT capture all valid versions (e.g., 1.2beta1)
          resource.version should fullyMatch regex """\d+(\.\d+){0,2}(-SNAPSHOT)"""
          resource.scalaVersion should be ("2.10.5")
    }
  }
}
