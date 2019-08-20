RELEASE_TYPE: patch

Adds `DynamoLockDaoBuilder` to provide `DynamoLockDao` from typesafe config.

Usage:

```scala
val lockDao = DynamoLockDaoBuilder.buildDynamoLockDao(config)
```
