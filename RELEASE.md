RELEASE_TYPE: minor

In `VersionedStore` modify the user provided update function to return an `Either`.

User defined error:

```scala
case class MyCaseClass(gimbleCount: 4)

val f = (t: MyCaseClass) => {
		if(t.gimbleCount > 2) {
				Left(UpdateNotApplied(new Throwable("BOOM!"))
		} else {
				Right(t.copy(gimbleCount = t.gimbleCount + 1)
		}
}

val result = update(id)(f)

// result: Left(UpdateNotApplied(new Throwable("BOOM!"))
```

Unexpected error:

```scala
case class MyCaseClass(gimbleCount: 4)

val f = (t: MyCaseClass) => {

		// Expected unexpected error!
		throw new Throwable("BOOM!")

		Right(MyCaseClass(0))
}

val result = update(id)(f)

// result: Left(UpdateUnexpectedError(new Throwable("BOOM!"))
```

Relevant update to type hierachy:

```scala
sealed trait UpdateError extends StorageError
sealed trait UpdateFunctionError extends UpdateError

case class UpdateNoSourceError(e: Throwable) extends UpdateError
case class UpdateReadError(e: Throwable) extends UpdateError
case class UpdateWriteError(e: Throwable) extends UpdateError

case class UpdateNotApplied(e: Throwable) extends UpdateFunctionError
case class UpdateUnexpectedError(e: Throwable) extends UpdateFunctionError
```
