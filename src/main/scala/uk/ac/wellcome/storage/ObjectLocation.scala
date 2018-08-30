package uk.ac.wellcome.storage

case class ObjectLocation(namespace: String, key: String) {
  override def toString = s"$namespace/$key"
}
