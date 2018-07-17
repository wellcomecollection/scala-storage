RELEASE_TYPE: patch

This patch fixes a bug in the `S3` fixture where it could attempt to create
buckets before the Docker container was responding to requests, leading
to intermittent test failures.
