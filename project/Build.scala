import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys.buildInfoPackage
import sbtbuildinfo.BuildInfoPlugin

object CeteraBuild extends Build {
  val Name = "come.socrata.cetera"

  lazy val commonSettings = Seq(
    scalaVersion := "2.11.7",
    // keeping 2.10 around during transition, once we're happy with 2.11 in prod we can remove it.
    crossScalaVersions := Seq("2.10.4", scalaVersion.value),

    scalacOptions ++= Seq("-Yinline-warnings"),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    initialize := {
      val jvRequired = "1.8"
      val jvCurrent = sys.props("java.specification.version")
      assert(jvCurrent == jvRequired, s"Unsupported JDK: java.specification.version $jvCurrent != $jvRequired")
    },

    fork in Test := true,
    testOptions in Test += Tests.Argument("-oDF"),
    resolvers ++= Deps.resolverList
  )

  lazy val build = Project(
    "cetera",
    file("."),
    settings = commonSettings
  )
    .aggregate(ceteraHttp)

  lazy val ceteraHttp = Project(
    "cetera-http",
    file("./cetera-http/"),
    settings = commonSettings ++ de.johoop.jacoco4sbt.JacocoPlugin.jacoco.settings ++ Seq(
      // Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
      fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") },
      fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") },
      buildInfoPackage := "com.socrata.cetera",
      libraryDependencies ++= Deps.http
    )
  )
    .disablePlugins(JmhPlugin)
    .enablePlugins(BuildInfoPlugin)

  lazy val perf = Project(
    "cetera-perf",
    file("./cetera-perf"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.perf
    )
  ).enablePlugins(JmhPlugin).dependsOn(ceteraHttp % "compile;test->test")
}

object Deps {
  lazy val resolverList = Seq(
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",
    "Artifactory release" at "https://repo.socrata.com/artifactory/simple/libs-release-local",
    "Artifactory snapshot" at "https://repo.socrata.com/artifactory/simple/libs-snapshot-local",
    "Cloudbees release" at "https://repository-socrata-oss.forge.cloudbees.com/release",
    "Cloudbees snapshot" at "https://repository-socrata-oss.forge.cloudbees.com/snapshot",
    Resolver.url("Artifactory ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns),
    Classpaths.sbtPluginReleases
  )

  lazy val http = common ++ logging ++ rojoma ++ socrata
  lazy val perf = common ++ rojoma

  lazy val common = Seq(
    "com.typesafe" % "config" % "1.0.2",
    "org.apache.lucene" % "lucene-expressions" % "4.10.3" % "test",
    "org.codehaus.groovy" % "groovy-all" % "2.3.5" % "test",
    "org.elasticsearch" % "elasticsearch" % "1.7.2",
    "org.mock-server" % "mockserver-maven-plugin" % "3.10.1" % "test"
  )

  // airbrake-java includes log4j
  lazy val logging = Seq(
    "org.slf4j" % "slf4j-log4j12" % "1.7.10",
    "io.airbrake" % "airbrake-java" % "2.2.8" % "provided"
  )

  lazy val rojoma = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.4.1",
    "com.rojoma" %% "simple-arm-v2" % "2.1.0"
  )

  lazy val socrata = Seq(
    "com.socrata" %% "socrata-http-client" % "3.5.0",
    "com.socrata" %% "socrata-http-jetty" % "3.5.0",
    "com.socrata" %% "socrata-thirdparty-utils" % "4.0.12",
    "com.socrata" %% "balboa-client" % "0.16.15"
  ).map { _.excludeAll(ExclusionRule(organization = "com.rojoma")) }
}
