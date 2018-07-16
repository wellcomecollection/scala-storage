resolvers += Resolver.jcenterRepo

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("org.scala-sbt" % "sbt-autoversion" % "1.0.0")
addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.34")
addSbtPlugin("ohnosequences" % "sbt-s3-resolver" % "0.19.0")
