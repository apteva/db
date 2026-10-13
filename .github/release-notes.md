AptevaDB alpha.4 fixes integer aggregation overflow and adds explicit non-null field counting.

- SUM/AVG accumulate integer inputs exactly in signed 128 bits, including parallel shard merges. Two maximum int64 values now produce a positive SUM of approximately 1.8446744073709552e19 and AVG of approximately 9.223372036854776e18. SUM returns int64 when the final total fits, otherwise float64; floating results have IEEE-754 precision. Cancellation after intermediate overflow preserves integer totals. Maintained computed sums retain their atomic final-int64-overflow rejection.
- `$countNonNull: "$field.path"` counts present non-null values, matching COUNT(column). Missing/null fields are excluded; zero, false, empty strings, arrays and objects count once. Existing row-count operations keep their behavior. Available in embedded queries, the Go HTTP client and native maintained computed collections.
- Sequential cursors reuse private value buffers and advance cached slots with less work. Inline callback scans copy keys/values together, use smaller cell references and bypass empty memtable merging. Saved-position resumption, range bounds and scan consistency remain covered by regression tests.

Format 9 permanently upgrades formats 3–8 using two durable checkpoints before new writes. **Back up with the original binary first; older binaries refuse the upgraded directory.** This guard protects persisted non-null-count definitions from older writers. Page, document and WAL layouts and logical export version 2 remain unchanged.

Install `github.com/apteva/db@v0.2.0-alpha.4` or download the Linux/macOS amd64/arm64 CLI/server archive and verify SHA256SUMS. This remains a public alpha; see README, USAGE, FORMAT and OPERATIONS for contracts and limitations.
