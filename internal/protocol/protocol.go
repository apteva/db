// Package protocol defines the version-one AptevaDB HTTP envelopes.
package protocol

import (
	"context"
	"encoding/json"
	"errors"
	db "github.com/apteva/db"
	"net"
	"net/url"
	"sort"
)

const MaxBodyBytes = 32 << 20

var ErrBadRequest = errors.New("aptevadb: bad request")
var ErrUnauthorized = errors.New("aptevadb: unauthorized")
var ErrBusy = errors.New("aptevadb: server at capacity")
var ErrProtocol = errors.New("aptevadb: invalid server response")

type Operation struct {
	Method     string `json:"method"`
	Collection string `json:"collection"`
	Key        []byte `json:"key"`
	Value      []byte `json:"value,omitempty"`
}
type Request struct {
	Op             string          `json:"op"`
	Collection     string          `json:"collection,omitempty"`
	Name           string          `json:"name,omitempty"`
	Field          string          `json:"field,omitempty"`
	Source         string          `json:"source,omitempty"`
	Transaction    string          `json:"transaction,omitempty"`
	Key            []byte          `json:"key,omitempty"`
	Value          []byte          `json:"value,omitempty"`
	Fields         []db.IndexField `json:"fields,omitempty"`
	NullsDistinct  bool            `json:"nulls_distinct,omitempty"`
	Sparse         bool            `json:"sparse,omitempty"`
	Sort           []db.IndexField `json:"sort,omitempty"`
	Hint           string          `json:"hint,omitempty"`
	KeyDescending  bool            `json:"key_descending,omitempty"`
	MaxMemoryBytes int64           `json:"max_memory_bytes,omitempty"`
	Unique         bool            `json:"unique,omitempty"`
	Pipeline       json.RawMessage `json:"pipeline,omitempty"`
	Definitions    json.RawMessage `json:"definitions,omitempty"`
	Filter         json.RawMessage `json:"filter,omitempty"`
	Skip           int             `json:"skip,omitempty"`
	Limit          int             `json:"limit,omitempty"`
	Operations     []Operation     `json:"operations,omitempty"`
}
type Response struct {
	Plan        *db.QueryPlan    `json:"plan,omitempty"`
	QueryStats  *db.QueryStats   `json:"query_stats,omitempty"`
	Error       *Error           `json:"error,omitempty"`
	Found       bool             `json:"found,omitempty"`
	Value       []byte           `json:"value,omitempty"`
	Key         []byte           `json:"key,omitempty"`
	Rows        []map[string]any `json:"rows,omitempty"`
	Collections []string         `json:"collections,omitempty"`
	Indexes     []db.IndexInfo   `json:"indexes,omitempty"`
	Stats       *db.Stats        `json:"stats,omitempty"`
	Transaction string           `json:"transaction,omitempty"`
	Version     string           `json:"version,omitempty"`
	Format      uint32           `json:"format,omitempty"`
	Done        bool             `json:"done,omitempty"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }

var codes = map[string]error{
	"key_exists": db.ErrKeyExists, "not_found": db.ErrNotFound, "duplicate_key": db.ErrDuplicateKey, "unique_value": db.ErrUniqueValue, "index_value": db.ErrIndexValue, "index_order": db.ErrIndexOrder, "conflict": db.ErrConflict, "closed": db.ErrClosed,
	"collection_missing": db.ErrNoSuchCollection, "index_missing": db.ErrNoSuchIndex, "index_exists": db.ErrIndexExists, "read_only": db.ErrReadOnly,
	"computed_read_only": db.ErrComputedCollectionReadOnly, "computed_dependency": db.ErrComputedCollectionDependency, "computed_limit": db.ErrComputedCollectionLimit,
	"computed_exists": db.ErrComputedCollectionExists, "document": db.ErrDocument, "collection_name": db.ErrCollectionName,
	"computed": db.ErrComputedCollection, "computed_field": db.ErrComputedField, "computed_nonempty": db.ErrComputedNonEmpty,
	"too_large": db.ErrTransactionTooLarge, "key_size": db.ErrKeySize, "value_size": db.ErrValueSize, "limit": db.ErrLimit, "pipeline": db.ErrPipeline,
	"cancelled": context.Canceled, "timeout": context.DeadlineExceeded, "bad_request": ErrBadRequest, "unauthorized": ErrUnauthorized, "busy": ErrBusy,
}

func (e *Error) Unwrap() error { return codes[e.Code] }
func FromError(err error) *Error {
	if err == nil {
		return nil
	}
	order := make([]string, 0, len(codes))
	for code := range codes {
		order = append(order, code)
	}
	sort.Strings(order)
	for _, code := range order {
		cause := codes[code]
		if errors.Is(err, cause) {
			return &Error{code, err.Error()}
		}
	}
	return &Error{"internal", err.Error()}
}
func LoopbackURL(u *url.URL) bool {
	host := u.Hostname()
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}
