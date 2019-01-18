RELEASE_TYPE: minor

The VHS no longer includes a shard in S3 keys.  This is based on
[updates to S3][no_shards_required] that mean sharding is no longer required:

> This S3 request rate performance increase removes any previous guidance to randomize object prefixes to achieve faster performance. That means you can now use logical or sequential naming patterns in S3 object naming without any performance implications.

[no_shards_required]: https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/
