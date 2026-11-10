AptevaDB alpha.22 exposes ReadRevision on a live database and Revision
on a snapshot. These in-process revisions let callers tie reusable
results to the exact state the query read, including computed-view and
catalog updates. Compare revisions only within one open database handle.

File format remains 10.
