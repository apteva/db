AptevaDB alpha.3 fixes saved-position cursor resumption and supports larger combined index entries.

- Reusing an index cursor for SeekAfter or SeekBefore at an earlier/later saved position now returns the same rows as a fresh cursor, after forward/reverse traversal. Sequential steps retain the leaf cache.
- Complete encoded index entries now support 8,160 bytes, independently of the 4,032-byte primary-key limit. The reported 1,500-byte key plus 3,000-byte indexed ASCII value fits. The encoded limit includes all compound fields, zero escaping, prefixes and suffixes; oversized writes/backfills fail atomically.
- Leaf/internal splits include the incoming cell to handle mixed entry sizes safely. Large index entries with empty values stay inline.

Format 8 permanently upgrades formats 3–7 using two durable checkpoints before new writes. Back up with the original binary first; older binaries refuse the upgraded directory. Page/WAL layouts and logical export version 2 remain unchanged.

Install github.com/apteva/db@v0.2.0-alpha.3 or download the Linux/macOS amd64/arm64 CLI/server archive and verify SHA256SUMS. This remains a public alpha; see README, USAGE, FORMAT and OPERATIONS for contracts and limitations.
