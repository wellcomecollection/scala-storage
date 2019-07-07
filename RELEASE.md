RELEASE_TYPE: patch

This replaces the generic `ObjectLocation` case class with a `NamespacedPath` trait and two concrete implementations: `S3ObjectLocation` and `S3ObjectLocationPrefix`.