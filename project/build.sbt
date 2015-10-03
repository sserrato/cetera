resolvers ++= Seq(
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  Classpaths.sbtPluginReleases
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.6")
addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.1.6")
