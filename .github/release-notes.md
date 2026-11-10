AptevaDB alpha.19 supports composite keys for stored computed collections.
Named scalar group components appear in the stored `_id` and as top-level
fields for filtering and indexing. Point reads use `ComputedGroupTupleKey`
with values in lexical alias order. Source edits maintain the rollup and its
indexes in the same transaction.

This release also exposes streaming query candidate estimates from B-tree
metadata. File format remains 10. Earlier versions cannot open a database
containing a stored composite definition.
