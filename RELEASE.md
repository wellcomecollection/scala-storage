RELEASE_TYPE: major

HybridRecord (the internal model used in the VHS) now stores the S3 bucket
name as well as the key.  This will break existing instances of the VHS.

There are also some new helpers on `LocalVersionedHybriStore`.