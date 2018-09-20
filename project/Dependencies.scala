import sbt._

object WellcomeDependencies {
  private lazy val versions = new {
    val json = "1.0.0"
  }

  val jsonLibrary: Seq[ModuleID] = Seq(
    "uk.ac.wellcome" % "json_2.12" % versions.json % "test",
    "uk.ac.wellcome" % "json_2.12" % versions.json % "test" classifier "tests"
  )
}

object Dependencies {

  lazy val versions = new {
    val aws = "1.11.225"
    val circe = "0.9.0"
    val guice = "4.2.0"
    val logback = "1.1.8"
    val mockito = "1.9.5"
    val scalaCheck = "1.13.4"
    val scalatest = "3.0.1"
    val scanamo = "1.0.0-M3"
    val apacheCommons = "2.6"
  }

  val circeDependencies = Seq(
    "io.circe" %% "circe-core" % versions.circe,
    "io.circe" %% "circe-parser"% versions.circe,
  )

  val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % versions.scalatest % Test,
    "org.mockito" % "mockito-core" % versions.mockito % Test,
    "com.google.inject.extensions" % "guice-testlib" % versions.guice % Test,
  )

  val scalacheckDependencies = Seq(
    "org.scalacheck" %% "scalacheck" % versions.scalaCheck % "test"
  )

  val loggingDependencies = Seq(
    "org.clapper" %% "grizzled-slf4j" % "1.3.2",
    "ch.qos.logback" % "logback-classic" % versions.logback,
    "org.slf4j" % "slf4j-api" % "1.7.25"
  )

  val diDependencies = Seq(
    "com.google.inject" % "guice" % versions.guice
  )
  
  val apacheCommons = Seq(
    "commons-io" % "commons-io" % versions.apacheCommons % "test")

  val libraryDependencies = Seq(
    "com.amazonaws" % "aws-java-sdk-dynamodb" % versions.aws,
    "com.amazonaws" % "aws-java-sdk-s3" % versions.aws,
    "com.gu" %% "scanamo" % versions.scanamo,
  ) ++
    circeDependencies ++
    loggingDependencies ++
    diDependencies ++
    scalacheckDependencies ++
    testDependencies ++
    apacheCommons ++
    WellcomeDependencies.jsonLibrary
}
