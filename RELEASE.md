RELEASE_TYPE: minor

This adds a new helper to the S3 fixture:

```scala
def getObjectFromS3[T](location: ObjectLocation)(implicit decoder: Decoder[T]): T
```
