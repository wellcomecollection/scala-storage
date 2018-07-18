import com.amazonaws.services.s3.model.Region

name    := "storage"
version := "1.2.0"

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

publishMavenStyle := true

publishTo := Some(
  "S3 releases" at "s3://releases.mvn-repo.wellcomecollection.org/"
)

publishArtifact in Test := true

enablePlugins(DockerComposePlugin)
