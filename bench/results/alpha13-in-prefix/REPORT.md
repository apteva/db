# Alpha.13: `$in` lists on compound-index prefixes

Alpha.12 let aggregations use compound indexes when the leading fields were
matched with `$eq`. Callers that pass an authorised scope as a list sent
`{"scope_id":{"$in":["internal"]}}`, which still scanned the collection.
Alpha.13 reads one index range per combination of listed values (at most 64).

The Database app's `BenchmarkScopedDayAggregates` uses 328,000 records of
about 600 bytes over 4 scopes and 26 days, with a `(scope_id, day)` index,
filtered to one day. The same Database build ran against alpha.12 and alpha.13,
interleaved, 3 runs each (ms; raw output beside this file). That Database
build already rewrites a one-value list to equality, so `in1` is fast on both.

| Scope filter | alpha.12 aggregate | alpha.13 aggregate | alpha.12 batch of 14 | alpha.13 batch of 14 |
|---|---:|---:|---:|---:|
| `eq` | 1.2–1.3 | 1.3–1.5 | 16.9–18.3 | 16.7–18.0 |
| one-value `in` | 1.2–1.4 | 1.3–1.4 | 16.9–21.6 | 17.6–32.9 |
| two-value `in` | 25.1–27.1 | 2.5–3.7 | 345–383 | 33–51 |

A two-scope list matches twice the records of one scope. Before the Database
rewrite, a one-value list on alpha.12 measured 23–27 ms per aggregate and
about 345 ms per batch.
