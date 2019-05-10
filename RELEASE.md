RELEASE_TYPE: minor

Add a new trait for database objects that manage versioning:

```scala
trait VersionedDao[T] {
  def put(value: T): Try[T]
  def get(id: String): Try[Option[T]]
}
```

See `InMemoryVersionedDao` for an example implementation.
