# Server and Go driver

The embedded API lives at `github.com/apteva/db`; the remote Go driver is
`github.com/apteva/db/client`. Both use the same error sentinels, binary documents
and collection concepts. The HTTP protocol is versioned at /v1; there is no SQL
adapter, replication or clustering in this alpha.

```sh
# Generate and keep a strong random token in your secret manager/environment.
export APTEVADB_TOKEN="$(openssl rand -hex 32)"
aptevadb-server -dir ./data -listen 127.0.0.1:5282
# For a remote listener:
aptevadb-server -dir ./data -listen :5282 -tls-cert server.crt -tls-key server.key
```

The token is read from the environment, never a command-line flag. The daemon
requires it even on loopback. Non-loopback listeners require TLS (minimum 1.2).
Use a valid certificate trusted by clients; do not disable certificate validation.
The single alpha token grants full database access; per-user/collection permissions
are not implemented. Protect the token accordingly. The daemon uses DurableOptions
and gracefully stops requests, releases transaction snapshots and closes the DB.

```go
remote, err := client.New("https://db.example.com:5282", client.Options{Token: token})
if err != nil { return err }
defer remote.Close()
err = remote.CreateCollection(ctx, "users")
if err != nil { return err }
users := remote.Collection("users")
err = users.InsertJSON(ctx, []byte("u1"), []byte(`{"name":"Ada","age":30}`))
if err != nil { return err }
var user struct { Name string; Age int }
found, err := users.FindDocument(ctx, []byte("u1"), &user)
```

Find/Insert/Upsert/Replace/Delete, typed/JSON writes, unique indexes, computed
fields/collections, aggregation, collection administration, atomic batches and
read/write transactions are available. Update callbacks run once; ErrConflict
requires retrying the whole transaction explicitly. Transaction collections support
point reads and writes; streaming queries/aggregation and DDL are outside remote
transactions in this alpha. Close streaming query cursors even on early exit.

The client pools connections, propagates contexts, refuses redirects and requires
HTTPS except for literal loopback/localhost. No request is automatically retried.
A transport failure after a write was sent returns ErrCommitUnknown: the operation
may have committed. Reconcile application keys before retrying. Stable primary
keys and create-only inserts help detect duplicates, but do not make uncertain
outcomes disappear. Transaction tokens expire after 60 seconds; disconnected
clients cannot retain snapshots indefinitely. Defaults bound concurrent requests,
transactions, body/frame size and aggregation rows. The handler's Options can tune
these limits. Context cancellation is checked before logging; once logging begins,
the engine completes its atomic write and required flush.

## HTTP v1 contract

Every endpoint requires `Authorization: Bearer <token>`. JSON requests use lower-case
field names. Keys/values are base64-encoded bytes. Values containing documents use
the engine's binary document encoding, not plain JSON; the Go driver handles it.

- GET /v1/health returns the library/format versions, or a failure envelope.
- POST /v1/rpc accepts `{ "op":"get", "collection":"users", "key":"dTE=" }`.
  Operations: collection, collections, drop_collection, get, insert, upsert,
  replace, delete, indexes, create_index, drop_index, computed_fields,
  computed_collection, aggregate, batch, stats, begin, commit and rollback.
- POST /v1/query accepts collection, filter ($match syntax), skip and limit.
  It streams NDJSON row frames followed by a required `{ "done":true }` frame.
  An error may appear in the terminal frame. EOF without that frame is failure.
- A begin response contains an opaque transaction token. Supply it as
  `transaction` on subsequent get/insert/upsert/replace/delete/commit/rollback.
  Transactions can touch several existing collections. Other operations with a
  transaction token are rejected rather than silently committing outside it.
- Error envelopes contain `error.code` and `error.message`. Go errors support
  errors.Is with engine sentinels. Validation is HTTP 400, constraint/conflict
  errors 409, authorization 401, and capacity/fatal health failures 503.

Requests reject unknown fields and trailing JSON. Responses may add optional
fields within v1. The driver never treats malformed/truncated responses as success.
Callbacks/queries have a 30-second server deadline by default; the default client
has a 35-second overall request timeout. HTTP body/header/idle/write timeouts must
also be used by embedders (`handler.NewHTTPServer` supplies them). Stop the HTTP
listener before closing the handler or underlying DB.

## Alpha.2 query/index additions

The Go driver exposes CreateCompoundIndex and ExplainQuery, and Query accepts
Sort, Hint, KeyDescending and MaxMemoryBytes. QueryOptions.Stats is filled from
the terminal stream frame. Fields serialize as `{ "field":"created", "desc":true }`.
IndexOptions includes NullsDistinct and Sparse. Typed document/large-integer
behavior matches the embedded API.

HTTP create_index accepts `fields`, `nulls_distinct` and `sparse`; explain accepts
the query filter/sort/hint/budget. Streaming query requests accept `sort`, `hint`,
`key_descending` and `max_memory_bytes`; the terminal frame includes `query_stats`.
The server caps requested working memory at Options.MaxQueryMemoryBytes (64 MiB
by default). Its frame/request limit is now 32 MiB, allowing base64 encoding of
16 MiB documents. Query row payloads are capped at 16 MiB. Memory budgets exclude
transport encoding/decoding and snapshot/page-cache storage. The embedded physical
IndexCursor API is available to adapters in the owning process; remote clients
use the ordered streaming Query API.


## Native change feeds (v0.2.0-alpha.5)

`POST /v1/watch` serves authenticated NDJSON subscriptions with collection/key/
operation/document filters, projection, replay tokens and bounded resource errors.
`watch_snapshot:true` creates an expiring read-only snapshot session whose get,
query and rollback operations use the existing transaction token field. Live
streams use a separate MaxWatchers limit (default 128), caller/server cancellation,
heartbeats and WatchWriteTimeout (default 30s) rather than the ordinary request
execution deadline. The Go client has separate watch connections and no overall
watch timeout. Capture is opt-in through DB.Options or daemon `-change-feed` and
persists across later opens; see [CHANGE_FEEDS.md](CHANGE_FEEDS.md).

### Document count RPC

`POST /v1/rpc` accepts `{"op":"count","collection":"orders",
"filter":{"bucket":"b00"},"skip":0,"limit":0}`. Optional `hint`,
`max_memory_bytes` and snapshot `transaction` lease IDs use the same envelopes
as queries. The response contains `count` (omitted for zero) and `query_stats`,
including `Plan.IndexOnly`, `DocumentFetches` and `IndexEntriesExamined`. The
server caps the requested memory budget at `MaxQueryMemoryBytes`. Native errors
and cancellation retain their existing protocol codes. Write transactions do
not support count; read-only snapshot leases do. Use the Go driver's
`Collection.CountDocuments(ctx, filter, db.CountOptions{})` for this operation.
