resolvers ++= Seq(
  "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release",
  Classpaths.sbtPluginReleases
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.2")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.1")