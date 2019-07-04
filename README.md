# scala-storage

[![Build Status](https://travis-ci.org/wellcometrust/scala-storage.svg?branch=master)](https://travis-ci.org/wellcometrust/scala-storage)

This is a Scala library for working with storage providers such as DynamoDB and S3.

## What it does

The library includes classes for:

- [`listing`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/listing): Listing things from a provider e.g. listing keys in an S3 bucket.

    ```scala
    trait Listing[Prefix, Result] {
      def list(prefix: Prefix): Either[ListingFailure, Iterable[Result]]
    }
    ```

- [`locking`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/locking): Distributed process locking.

    We run services in parallel, accessing shared data stores. In some cases, we only want one worker to be able to access those resources at a time. The locking service gives us a generic way of controlling access to resources. This is especially useful when the underlying resource does not support locking. For example, we use this to prevent concurrent writes to S3.

    This is how you use it:

    ```scala
    lockingService.withLocks(Set("1","2","3")) {
      // do some stuff
    }
    ```

    Here the locking service will try to acquire a lock on the identifiers 1, 2 and 3. It releases the locks when the process completes. If another worker tries to lock on any of these identifiers it will fail.


- [`maxima`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/maxima): Find the maximum valued thing in a store.

    For example, suppose we are storing multiple versions of files. We might have a database like this:

    ```
    filename        version
    =======================
    cats.gif        1
    cats.gif        2
    cats.gif        3
    rabbits.gif     1
    ```

    The `maxima` trait helps us find the latest version of each file. It can tell us that the latest version of `cats.gif` is 3, and the latest version of `rabbits.gif` is 1.

    In the DynamoDB implementation, it finds the greatest value of the range key for a given value of the hash key.

- [`store`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/store): We start with a base trait for getting and putting to a storage provider. Then we can compose this to add more operations in a generic way.

    - `StreamStore`: Getting/putting Java `InputStream` instances.

    - `TypedStore`: Getting/putting concrete types, such as Scala case classes. By default, it encodes case classes using JSON.

    - `HybridStore`: Combines two stores, one providing an index, the other a large object store. We store large JSON documents as part of a data pipeline, bigger than can be stored in DynamoDB. We use the `HybridStore` to store the JSON in S3, and a pointer to the S3 object in DynamoDB.

    - `VersionedStore`: It adds methods like `update()` to a generic store implementation, which means that a store can hold multiple versions. By implementing a put and a get, this provides the other methods for free.

    - `VersionedHybridStore`: A combination of the `HybridStore` and the `VersionedStore`, which allows us to store multiple versions of a large document with atomic updates.

- [`streaming`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/streaming): Convert Java/Scala classes to and from an `InputStream`.

- [`transfer`](https://github.com/wellcometrust/scala-storage/tree/master/storage/src/main/scala/uk/ac/wellcome/storage/transfer): Transferring things within a storage provider, including transfer by prefix

    For example, this allows us to copy a folder in S3 to another location.

## How it is written

### Traits AKA Type classes

Each trait contains its own logic, but implementations do not need to reproduce that logic. For example the `LockingService` contains logic to ensure that locks are always released even when errors occur.

An implementation of the `LockingService` need only define `lock()` and `unlock()` operations, the rest is provided by the trait.

### Composable test cases

Every trait comes with a set of generic test cases that an implementation must fulfill. A contract, if you like.

When we create a new implementation we can re-use those test cases. This means that we can be confident that implementations are interchangeable.

### In-memory implementations for testing

If we are testing code that calls a storage class, but we are not testing the storage class directly, we can use a simpler in-memory implementation. This is faster and simpler, and the composable test cases assure us that it has the same behaviour as an implementation we might use in production.

We prefer this approach to mocking as we feel it is simpler and the composable test cases provide a guarantee of correctness. We know that the behaviour of the test implementations always matches the "real" implementations.

### Using InputStream with storage providers

At the heart of our `Store` implementations is the `StreamStore` which enforces working with an InputStream when sending or receiving bytes from a storage provider. It doesn't care where the `InputStream` comes from: in-memory, a file on disk, a cloud provider, or something else. This makes it easier to test and more flexible.

Most of our services run on container hosts with limited disk space. Working with an `InputStream` without downloading the entire file means the disk space is not a problem.

## Where we use it

These libraries are used as part of the [Wellcome Digital Platform][platform].

[platform]: https://github.com/wellcometrust/platform

## Installation

This library is only published to a private S3 bucket.

Wellcome projects have access to this S3 bucket -- you can use our build
scripts to publish a copy to your own private package repository, or vendor
the library by copying the code into your own repository.

Read [the changelog](CHANGELOG.md) to find the latest version.
