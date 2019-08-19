RELEASE_TYPE: minor

Adds a:

- `DynamoSingleVersionStore` for storing a single version of a thing (where new versions replace old versions).
- `DynamoMultipleVersionStore` for storing multiple versions of a thing, (where all versions for an id are retained).
- Companion object for `MemoryTypedStore` to create instances easily



Example of `DynamoSingleVersionStore`:

1. Start with an empty table: 

        id      version     data
        ================================

2. Call `store.put()` with a `DynamoSingleVersionStore`, and a new row is written to the table:

        id      version     data
        ================================
        abcde   0           twas brillig

3. Call `store.put()` with the same id, and the row is replaced. The table only ever contains one row for a given id:

        id      version     data
        ================================
        abcde   1           slithy toves



Example of `DynamoMultipleVersionStore`:

1. Start with an empty table: 

        id      version     data
        ================================

2. Call `store.put()` with a `DynamoMultipleVersionStore`, and a new row is written to the table:

        id      version     data
        ================================
        abcde   0           twas brillig

3. Call `store.put()` with the same id, and a new row is added. The table contains a row for every (id, version) pair:

        id      version     data
        ================================
        abcde   0           twas brillig
        abcde   1           slithy toves

