import ohnosequences.sbt.SbtS3Resolver._
import com.amazonaws.services.s3.model.Region

organization := "uk.ac.wellcome"
name         := "storage"
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

lazy val project = Project(
  name,
  file("."),
  settings = Defaults.defaultSettings ++ S3Resolver.defaults)

s3region      := Region.EU_Ireland,
s3credentials := file("~/.aws/credentials"),
publishMavenStyle := false,
publishTo := Some(
  s3resolver.value(
    s"releases s3 bucket",
    s3(prefix+"mvn-repo.wellcomecollection.org")) withIvyPatterns
)
