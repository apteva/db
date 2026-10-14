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

## Compound indexes and ordered pages

Create indexes once during application setup. Field order matters; each component
can independently ascend or descend:

```go
err := orders.CreateCompoundIndex(ctx, "tenant_created", []db.IndexField{
    {Field: "tenant"},
    {Field: "created", Desc: true},
})
```

Compound indexes support scalar fields only. Missing values become null unless
Sparse is set. Arrays/objects in a compound component reject the write atomically;
ordinary CreateIndex retains its scalar/multikey behavior. New numeric indexes
order int64 values exactly, including adjacent values above 2^53. Mixed integer
and float comparisons retain all integer bits; finite numerically equal values
share an index key.

```go
stats := db.QueryStats{}
opts := db.QueryOptions{
    Sort: []db.IndexField{{Field: "created", Desc: true}},
    Limit: 100,
    MaxMemoryBytes: 8 << 20,
    Stats: &stats,
}
filter := []byte(`{"$and":[{"tenant":"acme"},{"status":{"$prefix":"paid."}}]}`)
plan, err := orders.ExplainQuery(ctx, filter, opts)
if err != nil { return err }
err = orders.Query(ctx, filter, opts, func(key, encoded []byte) bool {
    // Consume each borrowed row; decode only what your application needs.
    return true
})
```

Query returns primary-key order by default. Sort specifies lexicographic field
order; ties use ascending primary keys, or descending keys with KeyDescending.
An equality-bound leading index prefix can be omitted from Sort. A matching
compound index streams ordered pages; other sorts retain a bounded heap of
Skip+Limit rows, or the complete result when Limit is zero. Deep Skip still costs
work and retained space for a fallback sort. For reverse use of an existing
index, request the opposite field directions and KeyDescending=true; otherwise
create the desired directions explicitly to preserve ascending key ties.

Hint selects a compatible compound index. Hint="$natural" forces a collection
scan for diagnostics/comparisons. A sparse index cannot be hinted when it would
omit matching missing values. Unknown/incompatible hints fail. ExplainQuery
reports access, index, equality prefix, reverse traversal, memory budget and
whether sorting is required. QueryStats records examined/returned rows and
accounted retained query memory. Explain may count bounded index candidates for
ordinary scalar indexes; it does not execute the result query.

```go
cur, err := orders.NewIndexCursor(ctx, "tenant_created", db.IndexCursorOptions{
    Prefix: []any{"acme"},
})
if err != nil { return err }
defer cur.Close()
for ok := cur.First(); ok; ok = cur.Next() {
    // cur.Key() is the primary key; cur.Value() is the stored document.
    // cur.Position() is an unsigned opaque index position for SeekAfter.
}
if err := cur.Err(); err != nil { return err }
```

Index cursors expose First/Last, Next/Prev, Seek(tuple), SeekAfter/SeekBefore and
bounds in physical index order. Upper is exclusive unless UpperInclusive is true;
Lower is inclusive unless LowerExclusive is true. Partial tuples bound the whole
matching prefix. Compound cursors yield each document once; ordinary multikey
cursors yield physical entries and may repeat a document. Positions belong to
one index ID; an index dropped/recreated has a different ID. Signing, binding a
position to a filter/tenant and pagination across separate live snapshots belong
to the adapter. Snapshot cursors keep their original rows/catalogs through writes
and drops. Legacy format-6 indexes stay readable, but must be rebuilt under a new
name (then dropped) to use exact ordered cursors.

## Logical and literal-prefix filters

$and/$or accept nonempty arrays of filter objects and can nest to 32 levels.
Other fields at the same level combine with AND. $prefix matches literal,
case-sensitive string bytes; punctuation, percent signs and underscores have no
pattern meaning. Arrays in ordinary queries match if any scalar element matches.
Integer filter literals outside int64 are rejected; explicit decimals use finite
float64. Malformed filters and trailing JSON return ErrPipeline.

## Nullable uniqueness and budgets

IndexOptions{Unique:true, NullsDistinct:true} permits repeated null tuples, matching
common SQL nullable-unique behavior. Any null/missing compound component exempts
that tuple from uniqueness while retaining its ordered entry. The default remains
NullsDistinct=false: null is a constrained value. Ordinary indexes omit missing
fields; compound indexes normalize missing to null. Sparse=true omits a compound
tuple whenever any component is missing, but still indexes explicit nulls. These
options persist through snapshots, restart, backup and logical export/import.

QueryOptions.MaxMemoryBytes and Pipeline.SetLimits(db.Limits{MaxBytes: ...})
select retained working-memory budgets; zero selects 64 MiB. Parallel aggregation
workers share one budget. A limit error is ErrLimit, never partial writes.
Budgets include accounted execution storage and exclude page caches, snapshot
versions, the current borrowed input document, transport framing and caller
allocations. They are not process RSS limits; see OPERATIONS.md.

Documents now support 16 MiB encoded size, 4,096 fields, large strings/nested values
and up to 1,048,576 array elements, with nesting capped at 16. DocBuilder/ArrayBuilder
Bytes returns nil when a limit is exceeded; check Err() when using builders
manually. Typed/JSON write helpers return ErrDocument before persistence. Raw
Insert/Upsert remains an opaque byte API, so it does not validate document bytes.

Decode into map[string]any preserves int64 and float64 values directly, including
large integers. Struct numeric fields honor their declared Go types; interface
fields use json.Number. This avoids alpha.1 map decoding through float64.


Aggregation preserves integer sums across serial processing and parallel merges.
`$sum` returns `int64` when the final integer total fits and `float64` otherwise;
`$avg` returns `float64` (or null when there are no numeric inputs). Integer
subtotals use exact signed 128-bit accumulation, so cancellation after a large
intermediate sum retains precision. Final floating results have IEEE-754
precision. Both operators ignore missing, null and nonnumeric inputs.
Maintained computed collections retain their stricter contract: stored sums
require integer inputs and reject a final total outside int64 atomically.

Use `$countNonNull: "$field.path"` for the equivalent of `COUNT(column)`.
It excludes missing and null fields, and counts every other value once,
including zero, false, empty strings, arrays and objects. `$count: {}` counts
all rows, including those with missing/null fields. These are group accumulators;
the existing `$count` stage also continues to count rows.

```go
rows, err := orders.Aggregate(ctx, []byte(`[
  {"$group": {"_id": null,
    "rows": {"$count": {}},
    "priced": {"$countNonNull": "$amount"},
    "total": {"$sum": "$amount"},
    "mean": {"$avg": "$amount"}
  }}
]`))
```

The same non-null counter is supported in native computed collections and
through the Go HTTP client. Opening an existing database upgrades formats 3–8
to format 9; make a backup with the original binary before upgrading.

## Filtered document counts

```go
var stats db.QueryStats
n, err := orders.CountDocuments(ctx, []byte(`{"bucket":"b00"}`), db.CountOptions{
    Stats: &stats,
})
// For a covering (bucket ASC, rank ASC) scalar compound index:
// n == 5000, stats.Plan.Access == "index-count", stats.Plan.IndexOnly == true,
// stats.IndexEntriesExamined == 5000, stats.DocumentFetches == 0.
```

`CountDocuments` is available on embedded collections, the default `DB` and
`Snapshot` collection, and the HTTP Go driver's collections and leased snapshot
collections. It uses Query's JSON filter syntax and counts encoded documents,
not raw key/value records. Errors return zero rather than a partial count.
`Skip` skips matching documents; `Limit: 0` is unlimited. Counting requires no
sort or result array. For consistent multiple reads, use the same snapshot-bound
collection for each operation.

The initial covering path proves **all predicates** are scalar equality bindings
on a contiguous leading prefix of an exact compound index, including nested
`$and`. Ascending and descending components and exact integer/float comparisons
are supported. Non-sparse compound indexes normalize missing components to null,
matching Query's null-equality semantics. Sparse coverage requires non-null
equality bindings for every indexed component, so missing documents cannot be
silently omitted. Each scalar compound index has one entry per encoded document.
Checkpointed indexes count bounded leaf slots; snapshots with buffered inserts,
updates or deletes merge the snapshot's index entries before counting.

Residual fields, ranges, prefixes, `$or`, and scalar multikey/legacy indexes use
document evaluation. The fallback can narrow candidates through an applicable
compound index; multikey entries are never counted as extra documents. `Hint`
selects a candidate covering index, `$natural` forces document evaluation, and
an unknown hint returns `ErrNoSuchIndex`. If an existing hinted index cannot
prove full coverage, the fallback planner may choose a different index or a
collection scan. Hints cannot force an incorrect count.

`Stats` reports the access path, logical entries examined by the covering scan,
documents loaded for evaluation, matches returned, and retained query memory.
`IndexEntriesExamined` specifically measures the covering count path, including
skipped matches; it is zero on document-evaluation paths. `MaxMemoryBytes` defaults
to 64 MiB; negative budgets/skip/limit return `ErrPipeline`, and covering bounds
that exceed the budget return `ErrLimit`. As with Query, snapshots, page caches,
scan scratch buffers, filter/plan metadata and caller memory are outside this
retained-query-memory limit. No match-sized result buffer or deduplication set is
retained. Cancellation is checked between index leaf pages (or each merged entry)
and evaluated documents. Counts and their index catalog use one snapshot while
writers continue. Close snapshots after their operations have finished.

`COUNT(column)` remains the separate `$countNonNull: "$column"` aggregation
accumulator; `CountDocuments` counts matching documents, including null-valued
fields. Existing Query/aggregation calls retain their behavior; adapters must
adopt `CountDocuments` to use its covering path.
