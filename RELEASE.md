RELEASE_TYPE: minor

This patch adds three new helpers to the test fixtures:

```scala
trait S3 {
  def createS3ConfigWith(bucket: Bucket): S3Config
}

trait LocalDynamoDB {
  def createDynamoConfigWith(table: Table): DynamoConfig
}

trait LocalVersionedHybridStore {
  def createVHSConfigWith(table: Table, bucket: Bucket, globalS3Prefix: String): VHSConfig
}
```
