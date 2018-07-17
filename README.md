# scala-storage

Storage libraries in use at Wellcome comprising:

- `VersionedDao`: A DynamoDB wrapper allowing strongly typed _and_ strongly consistent updates.
- `ObjectStore`: A storage agnostic strongly typed large object store library (an `S3StorageBackend` is provided).
- `VersionedHybridStore`: A strongly typed _and_ strongly consistent large object store with indexes provided by DynamoDB.

[![Build Status](https://travis-ci.org/wellcometrust/scala-storage.svg?branch=master)](https://travis-ci.org/wellcometrust/scala-storage)

## Installation

This library is only published to a private S3 bucket.

Wellcome projects have access to this S3 bucket -- you can use our build
scripts to publish a copy to your own private package repository, or vendor
the library by copying the code into your own repository.

Read [the changelog](CHANGELOG.md) to find the latest version.
