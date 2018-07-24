RELEASE_TYPE: minor

This release changes the hashing algorithm used in `SerialisationStrategy`.

Previously we used MurmurHash3, which turned out to be more vulnerable to
collisions than we expected -- now we use SHA-256 instead.
