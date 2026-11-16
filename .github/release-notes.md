AptevaDB alpha.23 fixes two read-path stalls seen on multi-GB databases.

Background memory trimming no longer holds the database's exclusive lock
while it measures residency. It took that lock for a full mincore pass
over every mapped chunk once a second, stalling readers for 100-200 ms on
7 GB files. The scan now runs under a brief read snapshot, tracks idle
chunks between sweeps, and locks only to remap chunks that stayed cold.

A one-branch $or is now planned as its branch. Scope builders emit
{$or:[{centre:{$in:[...]}}]} for a single scope; the planner used to drop
it, so aggregates and counts lost their index prefix and scanned the whole
leading range. Real disjunctions are unchanged.

File format remains 10.
