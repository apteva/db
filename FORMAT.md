# On-disk format

Format version **8**, as `aptevadb.FormatVersion`. This document is the
contract for what a build writes and reads; the policy for changing it is at
the end.

## Directory

A database is a directory:

| File | Contents |
|---|---|
| `LOCK` | stable directory ownership across recovery and compaction swaps; never remove while open |
| `data` | fixed-size pages; pages 0 and 1 are the two meta pages |
| `wal-NNNNNNNN` | log generations, `NNNNNNNN` a zero-padded decimal generation number |
| `compact.tmp/` | a compaction's copy while it runs; removed at the next open |
| `data.old` | the previous file for the instant of a compaction swap; put back at the next open if `data` is missing, else removed |

`data` is locked with `flock` for the life of the process that opened it; a
second open fails with `ErrLocked`.

## Byte order

Page fields, document payloads and log fields are little-endian. Things
that must sort as bytes are big-endian: slot key prefixes, collection ids
inside key tags, index ids and primary-key lengths inside index entries.

## Pages

`PageSize` is 16384 bytes; page ids are 32-bit. Every page starts with a
16-byte header:

| Offset | Size | Field |
|---|---|---|
| 0 | u8 | type: 1 leaf, 2 internal, 3 overflow, 4 free list |
| 1 | u8 | level: 0 for leaves, 1 for parents of leaves, and so on |
| 2 | u16 | cell count (leaf, internal); chunk length (overflow); id count (free list) |
| 4 | u16 | lowest cell offset; cells grow down from the page end |
| 6 | u16 | bytes held by dead cells (leaf) |
| 8 | u32 | child 0 (internal); next page in the chain (overflow, free list) |
| 12 | u32 | crc32c (Castagnoli) of the page with this field zeroed |

Checksums are verified the first time a page is read after open, unless
`Options.NoVerifyChecksums` is set; a mismatch is `ErrCorrupt`.

### Leaf and internal pages

After the header comes an array of 12-byte slots, one per cell, in key
order: cell offset u16, key length u16, and the first 8 bytes of the key
big-endian, zero-padded. Searches compare slot prefixes and touch the cell
only on a tie.

A leaf cell is `klen u16, vlen u16, key, value`. A `vlen` of 0xFFFF marks an
overflow pointer: the 8-byte "value" is `total length u32, first overflow
page u32`. An internal cell is `klen u16, child u32, key`; child i of the
page (i ≥ 1) is cell i-1's child, child 0 is in the header, and every key
under child i is at least separator i-1 and below separator i.

Cells are never split across pages. Ordinary inline values retain the
`MaxCellSize = PageSize/4` threshold. Wider index keys may occupy a cell
with a small inline value or an overflow pointer. Every cell plus its slot
fits in half the usable page. Existing cuts are retained when the incoming
cell fits; otherwise the cut includes the incoming cell size, including
the promoted internal separator.

### Overflow pages

A value larger than a cell is stored as a chain of overflow pages: header,
then the chunk at offset 16, chunk length in the count field, next page in
the child field (0 ends the chain).

### Free-list pages

Free page ids are persisted as a chain of pages holding u32 ids from offset
16; the count field holds the number of ids in the page and the child field
the next page. The chain's head is in the meta page. A missing or damaged
chain is rebuilt by walking the tree.

### Meta pages

Pages 0 and 1 alternate; the one with the higher checkpoint number and a
valid checksum is current.

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `NDBMETA1` |
| 8 | u32 | format version |
| 12 | u32 | page size |
| 16 | u64 | checkpoint number |
| 24 | u32 | root page |
| 28 | u32 | page count (high-water mark) |
| 32 | u64 | log generation the replay starts from |
| 40 | u64 | offset in that generation |
| 48 | u32 | head of the free-list chain, 0 if none |
| 52 | u32 | crc32c of bytes 0 to 51 |

## Key space

Every key in the tree carries a one-byte tag.

| First byte | Meaning |
|---|---|
| 0x00 | reserved: catalog and index entries (below) |
| 0x01 | the default collection: `0x01` then the user key |
| 0x02 to 0xFE | collection id 1 to 253: `0x01 + id` then the user key |
| 0xFF | collection id 254 to 65535: `0xFF`, id u16 big-endian, then the user key |

User keys are 1 to `MaxKeySize` (4032) bytes and may contain any byte.
Values may be up to `MaxValueSize` (1 GiB). Complete encoded index keys
are limited to `MaxIndexKeySize` (8160) bytes, including the index prefix,
component encodings and escaped primary-key suffix. NUL bytes expand when
escaped; the limit applies after encoding. Oversized entries fail atomically
with `ErrKeySize`, on insertion, replacement and index backfill.

Reserved keys:

| Key | Value |
|---|---|
| `0x00 'g' collectionID_u32BE` | computed-field catalog: JSON `{"version":1,"fields":{"output":expression}}` |
| `0x00 'm' collectionID_u32BE` | computed-collection catalog: JSON `{"version":1,"source":sourceID,"pipeline":[...]}` |
| `0x00 'r' collectionID_u32BE groupKey` | grouped collection membership counter: positive uint64 little-endian |
| `0x00 'n' name` | collection catalog: JSON `{"id":n}` |
| `0x00 'c' coll u32 BE name` | index catalog: JSON `{"id":n,"field":"path"}` |
| `0x00 'i' index u32 BE, value encoding, primary key, pklen u16 BE` | index entry, empty value |

Legacy index definitions without an `encoding` property retain their original
value and primary-key encoding. That legacy value encoding orders like the executor's comparisons: a tag byte
that is the type's rank (null or missing 0, bool 1, number 2, string 3),
then for bool one byte, for numbers the float64 bits big-endian with the
sign bit flipped for positives and all bits inverted for negatives, and for
strings the bytes with 0x00 escaped as `0x00 0xFF` and terminated by `0x00
0x00`. Legacy integers use float64 buckets: values beyond 2^53 may share an entry;
exact original-value checks prevent false matches. New indexes use `encoding:2`.
Numeric values use a rank byte plus a sign category, normalized exponent u16BE
and uint64BE significand; negatives invert exponent/significand. This represents
all int64 bits and finite float64 values in one exact numeric order, canonicalizes
signed zero, and gives numerically equal integer/float values the same key.
Compound components concatenate their encodings; descending components invert
every byte. The primary-key suffix escapes zero as `00 FF`, terminates with
`00 00`, and ends in its encoded byte length u16BE. Index catalogs also preserve
`fields:[{field,desc}]`, `nulls_distinct` and `sparse`. Existing legacy indexes are
not silently rebuilt; ordered cursors require encoding 2.

## Documents

A document is a sequence of entries followed by a directory:

- entry: `name length u8, name, type u8, payload`
- directory: one u16 offset per entry, in entry order, then the entry count
  as a u8 (at most 255 entries)

Types and payloads: 1 null (none), 2 bool (1 byte), 3 int (int64), 4 float
(float64 bits), 5 string (length u16, bytes), 6 document (length u16, an
encoded document), 7 array (length u16, an encoded array). An array is an
element count u16 followed by `type u8, payload` per element. Nesting is
limited to 16 levels. Format 7 keeps that encoding for small values/documents.
An extended document has uint32LE offsets and a nine-byte footer:
field count uint32LE, magic uint32LE `0x31445741`, zero marker. Tags 8/9/10
encode strings/documents/arrays with uint32LE lengths. Large arrays use a zero
uint16 count, magic uint32LE `0x31415741`, then count uint32LE and elements.
Limits are 16 MiB encoded bytes, 4,096 fields and 1,048,576 array elements.
A legacy root document larger than 65,535 bytes is readable only if sequential
payload validation reconstructs every wrapped uint16 offset exactly. No payload
lengths are inferred. Values that do not decode as documents are ordinary
byte strings: they are stored, returned and iterated as such, and pipelines
skip them.

## Log

A generation file starts with a 16-byte header: magic `NDBWAL01` and the
generation number u64. Records follow:

```
length u32   length of the payload that follows the checksum
crc32c u32   over the payload
payload:     flags u8, key length u16, key, value
```

Flag 1 means another record of the same batch follows; flag 2 marks a
deletion. Replay applies a batch only when its last record (the first
without flag 1) is present and valid; a record with a bad checksum or a
truncated one ends replay, and the file is truncated there. The log rotates
at 64 MiB; generations older than the one the meta page names are deleted
after a checkpoint, and a generation left entirely dead by a full
checkpoint is dropped at close and after compaction.

## Compatibility policy

- `Open` reads formats 3 through 8. Older supported formats are upgraded with
  two synchronous full checkpoints, replacing both alternating meta slots
  before `Open` returns. Format 8 is durable before wider index cells
  can enter the WAL. Older binaries refuse it,
  including after a crash or a damaged newest meta page. This is a one-way
  upgrade; retain a backup for older binaries. Other versions are refused with
  `ErrFormat`. `FormatVersionOf` and `aptevadbctl format <dir>` report a file's
  version without opening it.
- Any change that an older build could misread bumps the version: page
  layout, key tags, catalog keys, the index value encoding, the document
  encoding, the log record layout.
- Adding a new page type, a new document type tag, a new reserved catalog
  prefix or a new log flag also bumps the version, since older builds would
  treat them as corruption.
- Format 4 adds the computed-field catalog and no layout changes. Version-one
  expressions use checked integer/finite float arithmetic and ASCII casing.
  Definitions are stored per collection; generated values are ordinary fields
  inside document records. Replay never evaluates expressions. Changes to these
  semantics require a new expression version and explicit migration.
- Format 5 adds computed-collection definitions and hidden group membership
  counters without changing page, log or document layouts. Source rows, dependent
  rows, counters and indexes share one WAL transaction. Group keys begin with
  their document scalar type tag, followed by bool byte, int64 little-endian,
  float64 bits little-endian or raw string bytes as appropriate. Missing IDs
  normalize to null; finite integral floats within int64 normalize to integers.
  Generated group documents store `_id` and checked int64 aggregates. Membership
  counters distinguish empty groups from groups whose sums are zero. Definitions
  support leading scalar matches and final projection or scalar grouping with
  integer sums/counts. Recovery does not re-evaluate pipelines. Changes to these
  semantics require a new definition version and explicit migration.
- Format 6 adds the `unique` flag to secondary-index catalog definitions and
  canonicalizes numeric signed zero in index keys. Older engines must refuse
  these files because they would otherwise write without enforcing uniqueness.
  The alpha API also changes Insert to create-only; Upsert retains replacement
  behavior. Readers also understand legacy signed-zero index keys; an update removes
  the old encoding and writes canonical zero. No full index rebuild is required. The test suite opens a real preserved format-5
  fixture with computed fields/collections, indexes and WAL, not just rewritten
  metadata headers.
- During 0.x, a bump ships with an automatic upgrade, a `aptevadbctl` conversion
  command or, when that is impractical, with a documented dump-and-reload through
  `aptevadbctl dump`; the changelog says which. From 1.0 on, every release
  reads the format written by the previous minor version.
- What is not part of the contract: `Stats` fields, lock-hold names, the
  exact page fill or page numbering a given load produces, and the
  in-memory structures.

- Format 7 adds extended document encodings, exact numeric/compound index
  encodings, nullable constraints and larger safe keys. No page or WAL record
  layouts change. Older writers refuse format 7. Export version 2 preserves the
  new catalogs; import version 1 remains supported.

- Format 8 permits complete encoded index entries up to 8,160 bytes. Page and
  WAL record layouts remain unchanged, but older split algorithms assume
  smaller cells, so older writers must refuse the directory. User keys remain
  limited to 4,032 bytes. Export/import stays at version 2.
