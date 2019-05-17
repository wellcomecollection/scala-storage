RELEASE_TYPE: major

This release adds a bunch of new type classes for database access, which should make mocking instances of VersionedDao easier in tests.

The new hierarchy is:

```scala
// Basic database access object
trait Dao[Ident, T] {
  def get(id: Ident): Try[Option[T]]
  def put(t: T): Try[T]
}

// Put conditions on the put() operation
trait ConditionalUpdateDao[Ident, T] extends Dao[Ident, T]

// The traditional VersionedDao
trait VersionedDao[T] {
  implicit val underlying: ConditionalUpdateDao[String, T]

  def get(id: String): Try[Option[T]] = underlying.get(id)

  def put(value: T): Try[T]
}
```
