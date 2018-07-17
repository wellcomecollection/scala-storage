# CHANGELOG

## v0.0.3 - 2018-07-17

This patch fixes a bug in the `S3` fixture where it could attempt to create
buckets before the Docker container was responding to requests, leading
to intermittent test failures.

## v0.0.2 - 2018-07-17

This patch fixes a bug in `S3StorageBackend`, where the following warning
would be emitted when uploading a file to S3:

> No content length specified for stream data.  Stream contents will be
> buffered in memory and could result in out of memory errors.

We're now using the S3 API correctly, so this error can't occur (and the
warning goes away).

## v0.0.2 - 2018-07-17

First release cut with the automated releases to S3!

This release fixes a race condition in the `LocalDynamoDb` fixture.  When
cleaning up a table, the fixture would clean up _every_ table -- even tables
created by a different test.  This caused intermittent test failures when
running tests in parallel.

## v0.0.1 - 2018-07-16

Initial release with the new S3 storage!
