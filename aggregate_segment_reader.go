package db

import (
	"hash/crc32"
	"io"
	"os"
)

type aggregateSegmentReader struct {
	f                                       *os.File
	next, offset, remaining, left, physical int64
	header                                  [16]byte
}

func (r *aggregateSegmentReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		if r.remaining != 0 || r.next != -1 {
			return 0, ErrCorrupt
		}
		return 0, io.EOF
	}
	if r.remaining == 0 {
		if r.next < 0 || r.next > r.physical-16 {
			return 0, ErrCorrupt
		}
		off := r.next
		if _, err := r.f.ReadAt(r.header[:], off); err != nil {
			return 0, err
		}
		if crc32.ChecksumIEEE(r.header[:12]) != le.Uint32(r.header[12:]) {
			return 0, ErrCorrupt
		}
		r.next = int64(le.Uint64(r.header[:]))
		r.remaining = int64(le.Uint32(r.header[8:]))
		if r.remaining <= 0 || r.remaining > r.left || r.remaining > r.physical-off-16 {
			return 0, ErrCorrupt
		}
		if r.next != -1 && (r.next < off+16+r.remaining || r.next > r.physical-16) {
			return 0, ErrCorrupt
		}
		r.offset = off + 16
	}
	n := int(min(int64(len(b)), r.remaining))
	got, err := r.f.ReadAt(b[:n], r.offset)
	r.offset += int64(got)
	r.remaining -= int64(got)
	r.left -= int64(got)
	return got, err
}
func (r *aggregateRead) nextGroup(spec *groupSpec) error {
	if r.stateSpec == nil {
		cost := int64(64 + len(spec.idFields)*64 + len(spec.accs)*80)
		if err := r.d.budget.reserve(cost); err != nil {
			return err
		}
		r.stateSpec = spec
		r.stateCost = cost
		r.state.id = make([]scalar, len(spec.idFields))
		r.state.accs = make([]accState, len(spec.accs))
	}
	return r.next(true)
}
