package db

import (
	"context"
	"testing"
)

func TestReadRevisionTracksSnapshot(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	mustCompat(t, err)
	t.Cleanup(func() { mustCompat(t, d.Close()) })
	ctx := context.Background()
	original, err := d.ReadRevision()
	mustCompat(t, err)
	held, err := d.Snapshot()
	mustCompat(t, err)
	defer held.Close()
	if held.Revision() != original {
		t.Fatal("initial snapshot revision mismatch")
	}
	c, err := d.Collection("facts")
	mustCompat(t, err)
	mustCompat(t, c.UpsertJSON([]byte("a"), []byte(`{"day":1,"caller":"x"}`)))
	afterWrite, err := d.ReadRevision()
	mustCompat(t, err)
	if afterWrite <= original || held.Revision() != original {
		t.Fatal("write did not advance the live revision independently of the held snapshot")
	}
	status, err := d.BeginComputedCollection(ctx, "totals", []ComputedCollectionInput{
		{Source: "facts", Pipeline: []byte(`[{"$group":{"_id":{"day":"$day"},"callers":{"$countDistinct":"$caller"}}}]`)},
	})
	mustCompat(t, err)
	beforeBuild, err := d.ReadRevision()
	mustCompat(t, err)
	for !status.Ready {
		status, err = d.AdvanceComputedCollection(ctx, "totals", 10)
		mustCompat(t, err)
	}
	afterBuild, err := d.ReadRevision()
	mustCompat(t, err)
	if afterBuild <= beforeBuild {
		t.Fatal("computed publication did not advance the revision")
	}
	visible, err := d.Snapshot()
	mustCompat(t, err)
	defer visible.Close()
	_, err = visible.Collection("totals")
	mustCompat(t, err)
	if visible.Revision() != afterBuild {
		t.Fatal("completed view snapshot revision mismatch")
	}
}
