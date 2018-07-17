RELEASE_TYPE: patch

This patch fixes a bug in `S3StorageBackend`, where the following warning
would be emitted when uploading a file to S3:

> No content length specified for stream data.  Stream contents will be
> buffered in memory and could result in out of memory errors.

We're now using the S3 API correctly, so this error can't occur (and the
warning goes away).
