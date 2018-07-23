RELEASE_TYPE: minor

This removes the `GlobalExecutionContext` from the library, an internal helper
that was never actually used, and a holdover from when this library was
part of the main platform repo.
