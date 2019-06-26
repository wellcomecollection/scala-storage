# scala-storage

[![Build Status](https://travis-ci.org/wellcometrust/scala-storage.svg?branch=master)](https://travis-ci.org/wellcometrust/scala-storage)

Provides type classes allowing interaction with storage service providers.

Storage providers currently supported:

- AWS DynamoDB
- AWS S3
- In memory

The library provides functionality for:

- Codec: provides generic encode/decode functionality for a type, using Json by default
- Listing: listing things from a provider
- Locking: safe distributed process locking around one or grouped identifiers
- Transfer: transferring things within a storage provider, including transfer by prefix
- Maxima: find the maximum valued thing where a numeric representation of that thing is provided
- Streaming: stream bytes to and from a storage provider
- Store: get and put to/from a storage provider
  - StreamStore: get and put to/from a storage provider that has a Streaming interface
  - TypedStore: get and put to/from a storage provider using concrete types where a Codec can be provided
  - HybridStore: combines two stores, one providing an index, the other typed storage
  - VersionedStore: get, put, update, upsert with a storage provider using concrete types using versions
  - VersionedHybridStore: Combining the index/object storage capability of the hybrid store with a VersionedStore

These libraries are used as part of the [Wellcome Digital Platform][platform].

[platform]: https://github.com/wellcometrust/platform

## Installation

This library is only published to a private S3 bucket.

Wellcome projects have access to this S3 bucket -- you can use our build
scripts to publish a copy to your own private package repository, or vendor
the library by copying the code into your own repository.

Read [the changelog](CHANGELOG.md) to find the latest version.
