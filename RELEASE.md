RELEASE_TYPE: minor

This removes all the logic for handling metadata in S3StreamStore and S3TypedStore, which we've never used.  We'll replace it with classes for interacting with S3 tags.