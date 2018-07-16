import sbt._

object Dependencies {

  lazy val versions = new {
    val logback = "1.1.8"
    val mockito = "1.9.5"
    val scalatest = "3.0.1"
    val circeVersion = "0.9.0"
    val guice = "4.2.0"
    val scanamo = "1.0.0-M3"
    val aws = "1.11.225"
    val akka = "2.5.9"
  }

  val circeDependencies = Seq(
    "io.circe" %% "circe-core" % versions.circeVersion,
    "io.circe" %% "circe-generic"% versions.circeVersion,
    "io.circe" %% "circe-generic-extras"% versions.circeVersion,
    "io.circe" %% "circe-parser"% versions.circeVersion,
    "io.circe" %% "circe-java8" % versions.circeVersion
  )

  val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % versions.scalatest % Test,
    "org.mockito" % "mockito-core" % versions.mockito % Test,
    "com.google.inject.extensions" % "guice-testlib" % versions.guice % Test,
    "com.typesafe.akka" %% "akka-actor" % versions.akka % Test,
    "com.typesafe.akka" %% "akka-stream" % versions.akka % Test
  )

  val loggingDependencies = Seq(
    "org.clapper" %% "grizzled-slf4j" % "1.3.2",
    "ch.qos.logback" % "logback-classic" % versions.logback,
    "org.slf4j" % "slf4j-api" % "1.7.25"
  )

  val diDependencies = Seq(
    "com.google.inject" % "guice" % versions.guice
  )

  val libraryDependencies = Seq(
    "com.amazonaws" % "aws-java-sdk-dynamodb" % versions.aws,
    "com.amazonaws" % "aws-java-sdk-s3" % versions.aws,
    "com.gu" %% "scanamo" % versions.scanamo,
  ) ++
    circeDependencies ++
    loggingDependencies ++
    diDependencies ++
    testDependencies
}
