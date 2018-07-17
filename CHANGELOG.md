# CHANGELOG

## v0.0.2 - 2018-07-17

First release cut with the automated releases to S3!

This release fixes a race condition in the `LocalDynamoDb` fixture.  When
cleaning up a table, the fixture would clean up _every_ table -- even tables
created by a different test.  This caused intermittent test failures when
running tests in parallel.

## v0.0.1 - 2018-07-16

Initial release with the new S3 storage!
