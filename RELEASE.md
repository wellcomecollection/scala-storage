RELEASE_TYPE: patch

Fix a bug in DynamoHashStore where getting the `max()` of an ID that didn't exist would return a `DoesNotExistError`, not a `NoMaximaValueError`.