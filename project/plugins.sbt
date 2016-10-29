resolvers ++= Seq(
  "socrata releases" at "https://repo.socrata.com/artifactory/libs-release",
  Classpaths.sbtPluginReleases
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.6")
addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.1.6")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.1.15")
