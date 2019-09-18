RELEASE_TYPE: patch

If you try to read an object from S3 with `S3StreamStore` and you get a rate limiting error from S3 (_Please reduce your request rate_), the error is marked as retryable.