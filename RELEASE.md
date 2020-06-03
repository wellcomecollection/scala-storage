RELEASE_TYPE: minor

Remove the code for handling storage classes from S3Transfer and S3PrefixTransfer; we manage all this with lifecycle rules on our S3 buckets.