RELEASE_TYPE: minor

This adds a new trait `Tags` for managing Tags in storage resources.
It includes in-memory and S3 implementations.

Examples:

```scala
val s3Tags: new S3Tags()

# Retrieve tags
s3Tags.get(objectLocation)

# Append tags
s3Tags.update(objectLocation) { existingTags =>
  existingTags ++ Map("newKey1" -> "newValue1")
}

# Replace tags
s3Tags.update(objectLocation) { _ =>
  Map("newKey1" -> "newValue1", "newKey2" -> "newValue2")
}
```