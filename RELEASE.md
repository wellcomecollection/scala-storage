RELEASE_TYPE: patch

*   In the VersionedHybridStore, when updating, don't write to the ObjectStore unless the underlying object has actually changed.

*   Make it easier to access underlying entries in MemoryVersionedDao

*   Add a way to delete objects from MemoryObjectStore