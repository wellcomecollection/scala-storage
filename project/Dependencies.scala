import sbt._

object WellcomeDependencies {
  private lazy val versions = new {
    val fixtures = "1.2.0"
    val json = "2.0.1"
    val typesafe = "2.0.0"
  }

  val fixturesLibrary: Seq[ModuleID] = Seq(
    "uk.ac.wellcome" % "fixtures_2.12" % versions.fixtures % "test",
    "uk.ac.wellcome" % "fixtures_2.12" % versions.fixtures % "test" classifier "tests"
  )

  val jsonLibrary: Seq[ModuleID] = Seq(
    "uk.ac.wellcome" % "json_2.12" % versions.json,
    "uk.ac.wellcome" % "json_2.12" % versions.json % "test" classifier "tests"
  )

  val typesafeLibrary = Seq[ModuleID](
    "uk.ac.wellcome" % "typesafe-app_2.12" % versions.typesafe,
    "uk.ac.wellcome" % "typesafe-app_2.12" % versions.typesafe % "test" classifier "tests"
  )
}

object Dependencies {

  lazy val versions = new {
    // Chosen to match the version used by Scanamo
    val aws = "1.11.504"

    val logback = "1.1.8"
    val mockito = "1.10.19"
    val scalatest = "3.1.1"
    val scalatestplusMockito = "3.1.0.0"
    val scanamo = "1.0.0-M10"
    val apacheCommons = "2.6"
  }

  val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % versions.scalatest % Test,
    "org.scalatestplus" %% "mockito-1-10" % versions.scalatestplusMockito % Test,
    "org.mockito" % "mockito-core" % versions.mockito % Test
  )

  val loggingDependencies = Seq(
    "org.clapper" %% "grizzled-slf4j" % "1.3.2",
    "ch.qos.logback" % "logback-classic" % versions.logback,
    "org.slf4j" % "slf4j-api" % "1.7.25"
  )

  val apacheCommons = Seq(
    "commons-io" % "commons-io" % versions.apacheCommons,
    "commons-io" % "commons-io" % versions.apacheCommons % "test"
  )

  val scanamoDependencies = Seq(
    "org.scanamo" %% "scanamo" % versions.scanamo,
    "org.scanamo" %% "scanamo-time" % versions.scanamo
  )

  val libraryDependencies: Seq[ModuleID] = Seq(
    "com.amazonaws" % "aws-java-sdk-dynamodb" % versions.aws,
    "com.amazonaws" % "aws-java-sdk-s3" % versions.aws
  ) ++
    loggingDependencies ++
    scanamoDependencies ++
    testDependencies ++
    apacheCommons ++
    WellcomeDependencies.jsonLibrary ++
    WellcomeDependencies.fixturesLibrary ++
    WellcomeDependencies.typesafeLibrary
}
