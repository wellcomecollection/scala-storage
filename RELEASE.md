# RELEASE_TYPE: minor

This release deprecates the following method in `LocalVersionedHybridStore`:

```scala
def getJsonFor[T](bucket: Bucket, table: Table, record: T, id: String): String
```

in favour of:

```scala
def getJsonFor(bucket: Bucket, table: Table, id: String): String
```

Both methods behave in the same way -- the `record` parameter in the original
method had no effect.  Callers can remove the `record` parameter to upgrade.
