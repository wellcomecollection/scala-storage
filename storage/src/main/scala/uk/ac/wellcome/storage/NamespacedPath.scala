package uk.ac.wellcome.storage

import java.nio.file.Paths

trait NamespacedPath {
  val namespace: String
  val path: String

  override def toString = s"$namespace/$path"

  protected def joinPaths(parts: String*): String =
    Paths.get(path, parts: _*).toString
}
