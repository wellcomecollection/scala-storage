import ohnosequences.sbt.SbtS3Resolver._
import com.amazonaws.services.s3.model.Region

name    := "storage"
version := "0.0.2"

organization := "uk.ac.wellcome"
scalaVersion := "2.12.6"

libraryDependencies := Dependencies.libraryDependencies

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

s3region := Region.EU_Ireland

publishMavenStyle := false

publishTo := Some(
  s3resolver.value(
    "releases s3 bucket",
    s3("releases.mvn-repo.wellcomecollection.org")) withIvyPatterns
)

publishArtifact in Test := true

enablePlugins(DockerComposePlugin)
