RELEASE_TYPE: patch

If you get a `DoesNotExistError` from S3StreamStore, you now get the underlying S3 exception rather than a generic java.lang.Error.