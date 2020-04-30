RELEASE_TYPE: minor

This adds a new argument to `Transfer.transfer()` and `PrefixTransfer.transferPrefix()`.

If you set `checkForExisting = false`, the transfer classes will skip checking whether an object already exists under a matching key.  This can be useful in S3, when making a GET to an object before it exists can [change the consistency](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html):

> Amazon S3 provides read-after-write consistency for PUTS of new objects in your S3 bucket in all Regions with one caveat. The caveat is that if you make a HEAD or GET request to a key name before the object is created, then create the object shortly after that, a subsequent GET might not return the object due to eventual consistency.
