RELEASE_TYPE: minor

This patch adds two helpers to the S3 fixture:

```scala
def getObjectFromS3[T](bucket: Bucket, key: String)(
  implicit decoder: Decoder[T]): T

def listKeysInBucket(bucket: Bucket): List[String]
```
