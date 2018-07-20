RELEASE_TYPE: minor

This patch adds a new helper to `S3` for saving a complete record of
everything in the bucket:

```scala
def getAllObjectContents(bucket: Bucket): Map[String, String]
```
