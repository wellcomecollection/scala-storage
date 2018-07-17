RELEASE_TYPE: minor

This adds a helper to `LocalVersionedHybridStore` that was missing from the
migration from the main repo:

```scala
def assertStored[T](bucket: Bucket, table: Table, id: String, record: T)(
  implicit encoder: Encoder[T])
```
