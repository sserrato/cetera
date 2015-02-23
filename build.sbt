import sbtassembly.Plugin.AssemblyKeys._ // put this at the top of the file

name := "Cetera"

organization := "com.socrata"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "socrata maven-snap" at "https://repository-socrata-oss.forge.cloudbees.com/snapshot",
  "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",
  Classpaths.sbtPluginReleases,
  "socrata internal maven" at "https://repo.socrata.com/artifactory/simple/libs-release-local",
  Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

libraryDependencies ++= Seq(
  "com.socrata" %% "socrata-http-jetty" % "3.0.0",
  "com.socrata" %% "socrata-http-client" % "3.0.0",
  "com.socrata" %% "socrata-thirdparty-utils" % "2.6.2",
  "com.rojoma" %% "rojoma-json-v3" % "3.2.2",
  "com.rojoma" %% "simple-arm-v2" % "2.0.0",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test" withSources() withJavadoc(),
  "org.scalacheck" %% "scalacheck" % "1.11.4" % "test" withSources() withJavadoc(),
  "com.typesafe" % "config" % "1.0.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "org.elasticsearch" % "elasticsearch" % "1.4.2"
)

resourceGenerators in Compile <+=
  (resourceManaged in Compile, version in Compile, scalaVersion in Compile) map { (dir, v, sV) =>
    val file = dir / "com/socrata/cetera" / "version"
    val contents = "{\"service\":\"%s\",\"version\":\"%s\",\"scala_version\":\"%s\"}".format("cetera", v, sV)
    IO.write(file, contents)
    Seq(file)
  }

initialCommands := "import com.socrata.cetera._"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

// WARNING: do not use -optimize if we start using akka
scalacOptions ++= Seq("-optimize", "-deprecation", "-feature", "-Xlint", "-Xfatal-warnings")

// Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d / "configs") }

fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "configs") }

net.virtualvoid.sbt.graph.Plugin.graphSettings

com.socrata.cloudbeessbt.SocrataCloudbeesSbt.socrataSettings(assembly = true)

ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 80

ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true
