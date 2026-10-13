# Go usage and contracts

Import `db "github.com/apteva/db"`. Open a local directory with
`db.Open(path, db.DurableOptions())`, check errors, and close the database.
`Options{}` is asynchronous; see DURABILITY.md before choosing it.

## Writes

| Operation | Missing key | Existing key |
|---|---|---|
| Insert / InsertJSON / InsertDocument | create | ErrKeyExists, unchanged |
| Upsert / UpsertJSON / UpsertDocument | create | replace |
| Replace / ReplaceJSON / ReplaceDocument | ErrNotFound | replace |
| Delete | success | remove |

The old Insert behavior is now Upsert. Replace replaces the whole value, not
selected fields. `Collection.UpdateDocument(ctx, key, callback)` safely reads,
edits a decoded map and replaces it in a transaction. An absent key returns
ErrNotFound. Errors support `errors.Is`.

`Collection(name)` creates a named collection if absent. `OpenCollection(name)`
only opens an existing collection. The empty name selects the default collection.
Collection IDs stay separate, so old live handles fail after a drop/recreation.
DropCollection and DropIndex are atomic, including after crash recovery. They
stage at most 256 MiB; a larger deletion returns ErrTransactionTooLarge without
changing anything. Removing large collections beyond this limit is not yet
supported as a single operation. Cancelled drops leave the original intact.

## Consistent reads

Each Scan, Query, aggregation or cursor reads one snapshot of rows and catalogs
for its whole operation. A held explicit Snapshot also makes several operations
share the same point in time. New collections/indexes are invisible to older
snapshots; dropped collections/indexes stay readable through those snapshots.
Point reads without a snapshot see the current committed value.

Close cursors (`defer cursor.Close()`), snapshots (`defer snapshot.Close()`) and
transactions (`defer tx.Rollback()`). Cursors own a snapshot unless created from
an existing one; snapshot cursors depend on the parent remaining open. Long-lived
views retain old page versions, increase memory, and prevent Compact.

Scan/Query callback bytes are borrowed until the callback returns. Copy retained
keys/values. Cursor Key/Value are borrowed until its next operation. Find copies
its value into the caller's destination buffer.

For many parallel point readers, use one `NewReader()` handle per worker and
close it. DB, collections, snapshots and Reader support concurrent operations;
Tx, Batch, Cursor and a mutable Pipeline belong to one goroutine. A single process
owns the directory exclusively; a second process cannot also open it.

## Transactions and batches

```go
err := database.Update(ctx, func(tx *db.Tx) error {
    accounts, err := tx.Collection("accounts")
    if err != nil { return err }
    var account struct { Balance int `json:"balance"` }
    found, err := accounts.FindDocument([]byte("alice"), &account)
    if err != nil { return err }
    if !found { return db.ErrNotFound }
    account.Balance -= 10
    return accounts.ReplaceDocument([]byte("alice"), account)
})
```

Begin/Commit/Rollback allow several reads and writes across existing collections.
Reads include staged source and computed outputs. Collections must be created
before Begin; schema changes are outside transactions. Writing transactions use
optimistic serializable validation: **any** concurrent committed write anywhere
in the database causes ErrConflict. This is conservative and may conflict on
unrelated records. A staged computed read may detect the conflict before Commit.
Retry the whole transaction explicitly if appropriate; Update
calls its callback once. Keep external side effects outside retryable callbacks.
Commit ends a transaction on success or failure. Rollback is idempotent.

For writes that need no preceding reads, NewBatch/With commit all operations in
one WAL group. Insert and Replace conditions are evaluated in queued order. An
error at Commit changes no records, indexes or computed outputs unless it is an
I/O failure after logging started; such a write has an uncertain outcome.

## Unique indexes

`CreateUniqueIndex(ctx, "email", "email")` or
`CreateIndex(ctx, "email", "email", db.IndexOptions{Unique:true})` validates the
existing data before publishing the index. Duplicate data leaves no index.
`ListIndexes()` reports uniqueness as well as names and fields.

Missing fields and non-scalar values have no index entry. Null is a value;
only one document may own it. Arrays yield scalar multikey entries: repeated
values within one document are allowed, shared values across documents fail.
Integers compare exactly, including beyond 2^53; mixed float/integer equality
follows the engine's numeric comparisons. Non-finite floats are refused. The
underlying numeric index remains an approximate float64 access path; original
values are compared for uniqueness and query predicates. Atomic swaps/deletions
within one batch or transaction are checked against the final state.

## Typed and streaming queries

InsertDocument/UpsertDocument/ReplaceDocument accept Go structs/maps using
encoding/json tags; FindDocument and FindOne decode into a struct or map.
Encode/Decode convert to/from the internal document format.

```go
err := users.Query(ctx, []byte(`{"age":{"$gte":18}}`),
    db.QueryOptions{Limit:100}, func(key, encoded []byte) bool {
        var user User
        if err := db.Decode(encoded, &user); err != nil { /* retain error; stop */ return false }
        // consume user; returning false stops early
        return true
    })
```

Query uses the $match filter syntax, applicable indexes and primary-key order.
Skip/Limit are nonnegative; zero Limit means unlimited. It streams without
retaining a result array. Aggregation supports group/sort/lookup and materializes
results under Pipeline Limits. SetLimits before sharing a compiled pipeline;
mutating the package-level DefaultLimits during concurrent execution is unsafe.
Snapshot queries preserve that snapshot's catalog and row versions.
