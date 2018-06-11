import sbt.Resolver
import sbtrelease.{Version, versionFormatError}

enablePlugins(GitVersioning)

organization := "uk.ac.wellcome"

name := "storage"

scalaVersion := "2.12.6"

val versions = new {
  val logback = "1.1.8"
  val mockito = "1.9.5"
  val scalatest = "3.0.1"
  val circeVersion = "0.9.0"
  val guice = "4.2.0"
  val scanamo = "1.0.0-M3"
  val aws = "1.11.95"
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

libraryDependencies := Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % versions.aws,
  "com.amazonaws" % "aws-java-sdk-s3" % versions.aws,
  "com.gu" %% "scanamo" % versions.scanamo,
) ++
  circeDependencies ++
  loggingDependencies ++
  diDependencies ++
  testDependencies

resolvers += Resolver.sonatypeRepo("releases")

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-Xlint",
  "-Xverify",
  "-feature",
  "-language:postfixOps"
)

publishTo := Some(Opts.resolver.sonatypeStaging)

useGpg := false

parallelExecution in Test := false

pgpPublicRing := baseDirectory.value / "pgp-key" / "pubring.asc"
pgpSecretRing := baseDirectory.value / "pgp-key" / "secring.asc"

releaseVersion := { ver: String =>Version(ver)
  .map(_.withoutQualifier)
  .map(_.bump(suggestedBump.value).string).getOrElse(versionFormatError)
}

enablePlugins(DockerComposePlugin)

// we hide the existing definition for setReleaseVersion to replace it with our own
import sbtrelease.ReleaseStateTransformations.{setReleaseVersion=>_,_}

// This creates a release stage that gets the version from the dynamic one
// based on tags generated by sbt-dynver plugin.
// Inspired from https://blog.byjean.eu/2015/07/10/painless-release-with-sbt.html
lazy val setReleaseVersion: ReleaseStep = { st: State =>
  val extracted = Project.extract(st)
  val currentVersion = extracted.get(version)

  val getReleaseVersionFunction = extracted.runTask(releaseVersion, st)._2
  val selectedVersion = getReleaseVersionFunction(currentVersion)

  st.log.info("Setting version to '%s'." format selectedVersion)
  val useGlobal =Project.extract(st).get(releaseUseGlobalVersion)
  val versionStr = (if (useGlobal) globalVersionString else versionString) format selectedVersion

  reapply(Seq(
    if (useGlobal) version in ThisBuild := selectedVersion
    else version := selectedVersion
  ), st)
}

git.baseVersion := "0.0.0"
git.useGitDescribe := true

// In sbt-autoversion plugin, minor is the default bump
// strategy as the minorRegexes matches any string.
// Let's override that so figuring out the next version fails if
// there are no commits that explicitly specify the bumping strategy
minorRegexes := List("""\[?minor\]?.*""").map(_.r)

releaseProcess := Seq(
  checkSnapshotDependencies,
  setReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  releaseStepCommand("sonatypeReleaseAll"),
  releaseStepCommand(s"git push origin --tags")
)