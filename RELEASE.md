RELEASE_TYPE: major

This is a major change to the way we do locking.

We now provide generic type classes `LockDao` and `LockingService`, and
concrete implementations as `DynamoLockDao` and `DynamoLockingService`.
