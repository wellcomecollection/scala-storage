RELEASE_TYPE: minor

Modify `S3Copier` so that if the destination object exists and is the same as the source object, we skip the CopyObject operation.

If the destination object exists and is different, `S3Copier` throws an error.
