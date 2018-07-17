RELEASE_TYPE: minor

This release adds two new fixtures to `LocalDynamoDb`:

```scala
def withSpecifiedLocalDynamoDbTable[R](
  createTable: (AmazonDynamoDB) => Table): Fixture[Table, R]

def withVersionedDao[R](table: Table)(testWith: TestWith[VersionedDao, R]): R
```

and some new helper methods for use in tests:

```scala
def givenTableHasItem[T: DynamoFormat](item: T, table: Table): Assertion

def getTableItem[T: DynamoFormat](id: String, table: Table): Assertion

def getExistingTableItem[T: DynamoFormat](id: String, table: Table): Assertion

def assertTableEmpty[T: DynamoFormat](table: Table): Assertion

def assertTableHasItem[T: DynamoFormat](
  id: String, item: T, table: Table): Assertion

def assertTableOnlyHasItem[T: DynamoFormat](item: T, table: Table): Assertion
```
