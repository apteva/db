package db

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"unicode/utf8"
)

type exportRecord struct {
	Kind       string                     `json:"kind"`
	Version    int                        `json:"version,omitempty"`
	Collection string                     `json:"collection,omitempty"`
	Fields     map[string]json.RawMessage `json:"fields,omitempty"`
	Computed   *ComputedCollectionInfo    `json:"computed,omitempty"`
	Indexes    []IndexInfo                `json:"indexes,omitempty"`
	Key        []byte                     `json:"key,omitempty"`
	Value      []byte                     `json:"value,omitempty"`
}

// Export writes a versioned, lossless JSON-lines logical backup. Binary keys
// and values are base64; computed outputs are rebuilt from source rows on import.
// Schemas, stored-field rules, computed collections and unique indexes are kept.
func (db *DB) Export(ctx context.Context, w io.Writer) error {
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := db.readSnapshot()
	if err != nil {
		return err
	}
	defer s.Close()
	encoder := json.NewEncoder(w)
	if err = encoder.Encode(exportRecord{Kind: "header", Version: 1}); err != nil {
		return err
	}
	names := make([]string, 0, len(s.collections))
	for n := range s.collections {
		if !utf8.ValidString(n) {
			return ErrCollectionName
		}
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		c := s.collections[name].bound(s)
		fields, err := c.ComputedFields()
		if err != nil {
			return err
		}
		computed, err := c.ComputedCollection()
		if err != nil {
			return err
		}
		if err = encoder.Encode(exportRecord{Kind: "collection", Collection: name, Fields: fields, Computed: computed, Indexes: c.ListIndexes()}); err != nil {
			return err
		}
	}
	for _, name := range names {
		c := s.collections[name].bound(s)
		if c.state.derived != nil {
			continue
		}
		var writeErr error
		err = c.Scan(nil, nil, func(k, v []byte) bool {
			if writeErr = ctx.Err(); writeErr != nil {
				return false
			}
			writeErr = encoder.Encode(exportRecord{Kind: "row", Collection: name, Key: k, Value: v})
			return writeErr == nil
		})
		if err != nil {
			return err
		}
		if writeErr != nil {
			return writeErr
		}
	}
	return encoder.Encode(exportRecord{Kind: "end"})
}

// Import creates a new directory from Export. It never replaces an existing
// directory. Failed/interrupted imports remain marked and cannot be opened.
// The export is trusted local input, not an unbounded network endpoint.
func Import(ctx context.Context, r io.Reader, destination string) error {
	absoluteDestination, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	destination = absoluteDestination
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(destination, 0755); err != nil {
		if os.IsExist(err) {
			return ErrDestinationExists
		}
		return err
	}
	if err := os.WriteFile(filepath.Join(destination, restoreMarker), []byte("AptevaDB import in progress\n"), 0600); err != nil {
		return err
	}
	if err := syncRestoreDirectory(destination); err != nil {
		return err
	}
	stage := filepath.Join(destination, "staged")
	database, err := Open(stage, DurableOptions())
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			_ = database.Close()
		}
	}()
	decoder := json.NewDecoder(r)
	var rec exportRecord
	if err = decoder.Decode(&rec); err != nil || rec.Kind != "header" || rec.Version != 1 {
		return fmt.Errorf("aptevadb: invalid export header")
	}
	schemas := map[string]exportRecord{}
	collections := map[string]*Collection{}
	batch := database.NewBatch()
	rowsStarted := false
	flush := func() error {
		if batch.Len() > 0 {
			return batch.CommitContext(ctx)
		}
		return nil
	}
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		rec = exportRecord{}
		if err = decoder.Decode(&rec); err != nil {
			return fmt.Errorf("aptevadb: incomplete export: %w", err)
		}
		switch rec.Kind {
		case "collection":
			if rowsStarted {
				return fmt.Errorf("aptevadb: schema after rows")
			}
			if _, exists := schemas[rec.Collection]; exists {
				return fmt.Errorf("aptevadb: duplicate collection schema")
			}
			schemas[rec.Collection] = rec
			if rec.Computed == nil {
				c, err := database.Collection(rec.Collection)
				if err != nil {
					return err
				}
				collections[rec.Collection] = c
				if len(rec.Fields) > 0 {
					definitions, _ := json.Marshal(rec.Fields)
					if err = c.DefineComputedFields(ctx, definitions); err != nil {
						return err
					}
				}
			}
		case "row":
			rowsStarted = true
			c := collections[rec.Collection]
			if c == nil {
				return fmt.Errorf("aptevadb: row has unknown or computed collection")
			}
			if err = batch.With(c).Insert(rec.Key, rec.Value); err != nil {
				return err
			}
			if batch.Len() >= 1000 || len(batch.buf) >= 4<<20 {
				if err = flush(); err != nil {
					return err
				}
			}
		case "end":
			if err = flush(); err != nil {
				return err
			}
			var extra any
			if err = decoder.Decode(&extra); err != io.EOF {
				return fmt.Errorf("aptevadb: trailing export data")
			}
			for name, schema := range schemas {
				if schema.Computed != nil {
					if len(schema.Fields) > 0 {
						return ErrComputedCollection
					}
					_, err = database.CreateComputedCollection(ctx, name, schema.Computed.Source, schema.Computed.Pipeline)
					if err != nil {
						return err
					}
				}
			}
			for name, schema := range schemas {
				c, err := database.OpenCollection(name)
				if err != nil {
					return err
				}
				for _, index := range schema.Indexes {
					if err = c.CreateIndex(ctx, index.Name, index.Field, IndexOptions{Unique: index.Unique}); err != nil {
						return err
					}
				}
			}
			if err = database.Verify(ctx); err != nil {
				return err
			}
			if err = database.VerifyIndexes(ctx); err != nil {
				return err
			}
			if err = database.Close(); err != nil {
				return err
			}
			completed = true
			if err = ctx.Err(); err != nil {
				return err
			}
			return publishStagedRestore(stage, destination)
		default:
			return fmt.Errorf("aptevadb: unknown export record %q", rec.Kind)
		}
	}
}

func publishStagedRestore(stage, destination string) error {
	entries, err := os.ReadDir(stage)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = os.Rename(filepath.Join(stage, entry.Name()), filepath.Join(destination, entry.Name())); err != nil {
			return err
		}
	}
	if err = os.Remove(stage); err != nil {
		return err
	}
	if err = syncRestoreDirectory(destination); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(destination, restoreMarker)); err != nil {
		return err
	}
	if err = syncRestoreDirectory(destination); err != nil {
		return err
	}
	return syncRestoreDirectory(filepath.Dir(destination))
}
