RELEASE_TYPE: patch

In the VersionedHybridStore, when updating, don't write to the ObjectStore unless the underlying object has actually changed.