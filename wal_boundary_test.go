package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestWALOrdinaryBoundaryPreservesRecordsWithinBufferBudget(t *testing.T) {
	for _, raw := range []bool{false, true} {
		for _, heap := range []bool{false, true} {
			t.Run(fmt.Sprintf("raw=%v/heap=%v", raw, heap), func(t *testing.T) {
				w := newWAL(t.TempDir())
				w.heapOnly = heap
				if err := w.start(1, 0); err != nil {
					t.Fatal(err)
				}
				defer w.close()
				first := bytes.Repeat([]byte{37}, walMaxBuffered-11-len("first")-32)
				second := bytes.Repeat([]byte{73}, 512)
				put := func(k string, v []byte) error {
					if raw {
						_, err := w.appendRaw(encodeRecord(nil, []byte(k), v, false, false, w.crcTab))
						return err
					}
					_, err := w.append([]byte(k), v, false)
					return err
				}
				if err := put("first", first); err != nil {
					t.Fatal(err)
				}
				if err := put("second", second); err != nil {
					t.Fatal(err)
				}
				if len(w.bufStore) > walMaxBuffered {
					t.Fatalf("ordinary records allocated %d WAL bytes, bound %d", len(w.bufStore), walMaxBuffered)
				}
				if err := w.flush(); err != nil {
					t.Fatal(err)
				}
				seen := 0
				_, _, err := w.replayFrom(1, walHeaderSize, func(k, v []byte, del bool) error {
					want, name := first, "first"
					if seen == 1 {
						want, name = second, "second"
					}
					if string(k) != name || del || !bytes.Equal(v, want) {
						return fmt.Errorf("invalid record %d", seen)
					}
					seen++
					return nil
				})
				if err != nil || seen != 2 {
					t.Fatalf("replay rows=%d err=%v", seen, err)
				}
			})
		}
	}
}
