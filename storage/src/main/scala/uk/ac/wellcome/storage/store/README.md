# storage

A `Store` is a generic trait describing a class that can put/get instances of a type `T`.

```scala
trait Store[Ident, T] {
  def get(id: Ident): ReadEither

  def put(id: Ident)(t: T): WriteEither
}
```

A `StreamStore` is a store specifically designed to pass around instances of `java.net.InputStream`.

```scala
trait StreamStore[Ident, IS <: InputStream with HasLength] {
  def get(id: Ident): ReadEither

  def put(id: Ident)(t: IS): WriteEither
}
```