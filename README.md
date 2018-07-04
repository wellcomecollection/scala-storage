# scala-storage

Storage libraries in use at Wellcome comprising:

- `VersionedDao`: A DynamoDB wrapper allowing strongly typed _and_ strongly consistent updates.
- `ObjectStore`: A storage agnostic strongly typed large object store library (an `S3StorageBackend` is provided).
- `VersionedHybridStore`: A strongly typed _and_ strongly consistent large object store with indexes provided by DynamoDB.

[![Build Status](https://travis-ci.org/wellcometrust/scala-storage.svg?branch=master)](https://travis-ci.org/wellcometrust/scala-storage)

## Installation

```scala
libraryDependencies ++= Seq(
  "uk.ac.wellcome" %% "storage" % "<LATEST_VERSION>"
)
```

`storage` is published for Scala 2.11 and Scala 2.12.

Read [the changelog](CHANGELOG.md) to find the latest version.

## Development

If you want to release this library you'll need credentials to authenticate with Travis and Sonatype.

### Releasing to Sonatype

Create a file `credentials.sbt` in the root of the repo with the following contents (but with the correct details).

```sbt
credentials += Credentials("Sonatype Nexus Repository Manager",
       "oss.sonatype.org",
       "(Sonatype user name)",
       "(Sonatype password)")

pgpPassphrase := Some("(PGP password)".toCharArray)
```

Then run `publishSigned` and `sonatypeRelease` in sbt to push a release:

```sh
sbt ++2.11.11 publishSigned sonatypeRelease;
```
