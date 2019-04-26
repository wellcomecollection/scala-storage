package uk.ac.wellcome.storage

import java.nio.file.Paths

case class ObjectLocation(namespace: String, key: String) {
  override def toString = s"$namespace/$key"

  def join(parts: String*): ObjectLocation = this.copy(
    key = Paths.get(this.key, parts: _*).toString
  )
}
