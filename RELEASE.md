RELEASE_TYPE: patch

Fix a flaky test in DynamoHybridStore.

Also, S3Fixtures now has a `createInvalidBucketName` method, which returns a string which is guaranteed not to be a valid S3 bucket name.