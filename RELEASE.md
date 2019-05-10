RELEASE_TYPE: minor

This modifies the StorageBackend and ObjectStore traits to use `Try` instead of `Future`.

It also pushes more of the logic inside ObjectStore itself, is better tested, and should make it a bit easier to write tests that require an ObjectStore.