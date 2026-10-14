# Native change feeds

Available in alpha.5 development source. Public alpha.4 does not contain these
APIs. Root engine, server and Go client remain standard-library-only.

## Enable capture

```go
opts := db.DurableOptions()
opts.ChangeFeed = &db.ChangeFeedOptions{
    RetentionBytes: 256 << 20,
    RetentionAge: time.Hour,
}
database, err := db.Open("./data", opts)
```

A fresh database with nil ChangeFeed has capture disabled. Settings persist;
nil on a later Open preserves them, including opens through the CLI. To end the
old history, explicitly open with `ChangeFeedOptions{Disabled: true}`. Enabling
again creates a new identity and invalidates previous tokens. Changing retention
on an enabled database preserves its identity but can expire older tokens.

The daemon accepts `-change-feed`, `-change-history-bytes` and
`-change-history-age`. Its default still preserves existing stored settings.
Capture is not enabled automatically on fresh databases.

## Watch committed writes

```go
feed, err := database.Watch(ctx, db.WatchOptions{
    Collections: []string{"orders"},
    Operations: []db.ChangeOp{db.ChangePut},
    Filter: []byte(`{"tenant":"acme","status":"paid","total":{"$gte":100}}`),
    IncludeValues: true,
    Fields: []string{"status", "total"},
    After: savedToken, // empty: start after the current applied state
})
if err != nil { return err }
defer feed.Close()

for feed.Next() {
    change := feed.Change()
    switch change.Operation {
    case db.ChangePut:
        // Replace the local projected row under change.Key.
    case db.ChangeDelete:
        // Remove the local row.
    case db.ChangeReset, db.ChangeDrop:
        // Refresh or discard this collection's local state.
    }
    if change.EndOfCommit {
        // Persist change.CommitToken after successfully processing the commit.
    }
}
return feed.Err()
```

`Collection.Watch` supplies its collection name. Empty Collections watches all
user collections, including the default collection (name "") and maintained
computed collections. Names are literal; name-based watches can observe a Drop
and a subsequent Reset if that name is recreated. Unknown names can be watched
before creation. Internal index/catalog/maintenance keys are never row events.

Metadata-only delivery is the default. Put covers insertion and replacement;
unchanged upserts and absent deletes can appear. Repeated writes to one key
within a commit are coalesced to its final operation, in first-seen key order.
Events within a commit need not follow the order of the intermediate writes.

Change buffers are read-only and borrowed until the next Next or Close. Use
`change.Clone()` when handing them to another goroutine. `change.Decode(&out)`
decodes binary documents when HasValue is true; opaque raw values use Value.
Next/Change/Err belong to one goroutine; Close and ResumeToken are safe concurrently.

## Filtering and projection

Filters reuse Query predicates: exact int64 comparisons, equality/ranges,
`$in`, `$and`, `$or`, literal `$prefix`, nested paths and array semantics.
Document predicates inspect Put's exact committed value. They do not fetch the
latest database row. Delete has no post-image and cannot match a document filter.
Filter size, nesting and node limits apply (64 KiB, depth 32, 1,024 nodes/items).

KeyPrefix and Operations filter row metadata before document access. Fields is
a top-level inclusion projection and requires IncludeValues. Collection scope
also filters Reset/Drop; these invalidation events pass regardless of key,
operation or document filters so a subscription cannot silently keep obsolete
state. Creation/backfill/rule changes use one Reset rather than flooding the
feed with initial rows.

Identical match specifications share compiled plans and bounded hot-commit
bitsets. Small projected results and replay payloads are shared where possible.
Document filtering/projection happens in readers outside the tree writer lock.
Hot values are copied once only when value-consuming subscriptions need that
collection; metadata-only subscribers do not require that copy.

For live query membership, watch a computed collection whose `$match` defines
membership and `$project` defines its stored row. Entering/changing produces Put;
leaving produces Delete in the same commit as the source write. An after-image
filter alone does not notify you when a row ceases to match.

## Initial state without a gap

```go
snapshot, feed, err := database.SnapshotAndWatch(ctx, opts)
if err != nil { return err }
defer feed.Close()
// Read collections through snapshot, using the same application predicate.
// Close snapshot after initial reads; it retains old tree pages while open.
snapshot.Close()
// Continue with feed.Next().
```

After must be empty for this operation. Snapshot and feed boundary are captured
under one writer lock. Sync mode fences the boundary before returning. If the
history expires before the initial read completes, the feed reports expiry and
the application starts again; it never silently omits intervening changes.

The Go HTTP client exposes the same Watch/Change interface. Its
SnapshotAndWatch returns a read-only lease with Collection.Find/FindDocument/
FindJSON/Query and Close. The server's TransactionTTL and MaxTransactions apply
to this lease. It is also released when the live stream ends. Snapshot handles,
like transactions, belong to one goroutine. Finish queries before Close.

## Ordering, replay and durability

Commits are delivered in WAL order, without interleaving events from different
commits. EndOfCommit marks the end of the selected changes, not intermediate
source/index updates. Skipped commits advance ResumeToken; coalesced Progress
control frames make that progress visible over HTTP (default heartbeat 15s).
Applications should ignore Progress as a row mutation but can checkpoint it.

Tokens are opaque, versioned and bound to the capture identity and original
commit marker. Resume starts strictly after that commit. Store a token only
once the whole commit has been processed; reconnecting after partial processing
replays that commit. Consumers must tolerate duplicates: delivery is at least
once within retained history, not exactly once. Tokens have integrity checks;
they are not permission grants or signatures.

Rejected constraints, conflicts, validation/capture-limit failures and rollback
produce no events. Normal Sync writes publish only after the durability fence;
async writes publish after atomic in-memory application and may be lost on
crash/power loss, just like async data. I/O failures can have an unknown commit
outcome; recovery can restore a complete committed group even if its caller saw
an error. An old token from a lost tail cannot alias a replacement write.

## Bounded history and working memory

The feed retains the original WAL. A small commit marker stores final-operation
references, collection names, keys, identity and time; document values are not
logged twice. Checkpoint/compaction preserve usable history. Backup/restore starts
a fresh stream identity and does not copy change history; logical export/import
continues to cover user data/definitions rather than capture settings/history.

RetentionBytes is a logical byte window; RetentionAge limits replay age. Fully
checkpointed WAL files are reclaimed at segment boundaries. Physical retained
WAL can exceed the logical window by its boundary segment plus the active tail
needed for database recovery, especially for a large individual write. These
limits do not delete data needed for recovery. A commit larger than the history
window can immediately expire a lagging cursor; size the window for the workload.

There are no permanent per-subscriber queues and readers do not pin old WAL
file descriptors across application delays. Defaults:

| Setting | Default | Scope |
|---|---:|---|
| RetentionBytes | 256 MiB | Shared logical history |
| RetentionAge | 1 hour | Replay age |
| CacheBytes | 8 MiB | Shared hot commit cache |
| MaxWatchBytes | 64 MiB | Compiled plans and reader working buffers |
| MaxWatchers | 256 | Embedded cursors |
| MaxCaptureBytes | 256 MiB | Commit metadata/coalescing work |
| WatchOptions.MaxMemoryBytes | 64 MiB | One cursor's working budget |

These are accounting limits, not a promise about total RSS or Go GC peaks.
Existing data/page/WAL buffers, temporary validation and application copies also
use memory. Snapshot page retention is separate. Compiled match plans and per-cursor
projection plans are charged, including when their match plan is shared. Resource exhaustion reports
ErrLimit (or ErrWatchLimit for cursor count), never partial/truncated values.
Capture-budget exhaustion rejects the write before logging; reader-budget
exhaustion terminates only that reader. Large metadata commits may require a
larger cursor/global budget even though delivery is incremental. Values larger
than a cursor budget remain usable through the engine but cannot be included by
that cursor; metadata-only subscriptions are available.

ErrHistoryExpired requires a new snapshot. ErrResumeInvalid means a malformed,
foreign or replaced-history/lost-tail token. Slow readers fail explicitly when
they leave retained history and cannot hold writers hostage indefinitely.

## HTTP transport

Authenticated POST /v1/watch accepts:

```json
{"op":"watch","watch":{"collections":["orders"],"filter":{"status":"paid"},"include_values":true,"fields":["status","total"],"after":""}}
```

It returns application/x-ndjson: one WatchReady/ChangeToken frame, Change frames,
then a terminal Done/Error frame if the connection remains writable. Binary keys
and values are base64, as in the existing protocol. watch_snapshot:true includes
a read-only snapshot session token in the ready frame; get/query/rollback use it.
A failed/truncated stream never advances the client beyond a completed commit.

Live streams have separate capacity slots (server default 128) and a separate Go
client connection pool, so they cannot occupy ordinary CRUD slots/connections.
The Go client removes the overall HTTP timeout for watches while retaining header
limits, caller cancellation and refusal of redirects. The server retains bounded
request parsing, per-frame size limits and a write-idle deadline refreshed with
heartbeats. Server.Close and Client.Close cancel live streams. HTTP permits
16 MiB document values under the existing 32 MiB frame cap; larger raw embedded
values require metadata delivery or a suitable embedded budget.

## Format migration

Format 10 adds a WAL-only final commit marker (flag bit 2). It is never a tree
key and replay applies no part of a group until this final marker is complete.
Opening formats 3–9 upgrades both metadata slots durably before capture can
start. Older binaries refuse the upgraded directory. Back up with the original
binary first. Logical export/import remains version 2.
