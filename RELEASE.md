RELEASE_TYPE: patch

First release cut with the automated releases to S3!

This release fixes a race condition in the `LocalDynamoDb` fixture.  When
cleaning up a table, the fixture would clean up _every_ table -- even tables
created by a different test.  This caused intermittent test failures when
running tests in parallel.
