name := "cetera"

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
  "com.rojoma" %% "rojoma-json-v3" % "3.3.0",
  "com.rojoma" %% "simple-arm-v2" % "2.1.0",
  "org.scalacheck" %% "scalacheck" % "1.11.4" % "test" withSources() withJavadoc(),
  "com.typesafe" % "config" % "1.0.2",
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10",
  "org.elasticsearch" % "elasticsearch" % "1.7.2"
)

initialCommands := "import com.socrata.cetera._"

enablePlugins(sbtbuildinfo.BuildInfoPlugin)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-Yinline-warnings")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
// TODO: enable scalastyle build failures
com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false

// Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "configs") }
fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d / "configs") }

de.johoop.jacoco4sbt.JacocoPlugin.jacoco.settings

initialize := {
  val jvRequired = "1.8"
  val jvCurrent = sys.props("java.specification.version")
  assert(jvCurrent == jvRequired, s"Unsupported JDK: java.specification.version $jvCurrent != $jvRequired")
}
