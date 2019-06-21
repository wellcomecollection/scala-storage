package uk.ac.wellcome.storage

import java.nio.file.Paths

case class ObjectLocation(namespace: String, path: String) {
  override def toString = s"$namespace/$path"

  def join(parts: String*): ObjectLocation = this.copy(
    path = Paths.get(this.path, parts: _*).toString
  )

  def asPrefix: ObjectLocationPrefix =
    ObjectLocationPrefix(
      namespace = namespace,
      path = path
    )
}

case class ObjectLocationPrefix(namespace: String, path: String) {
  def asLocation(parts: String*): ObjectLocation =
    ObjectLocation(
      namespace,
      path = Paths.get(this.path, parts: _*).toString
    )
}
