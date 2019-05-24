RELEASE_TYPE: major

This release modifies most of the data store methods to return `Either[Error, T]` instead of `Try[Option[T]]`, so we can more easily distinguish between the error cases "doesn't exist" and "other error".
