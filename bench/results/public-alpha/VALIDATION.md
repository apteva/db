# AptevaDB public-alpha validation

Build: `f97b8944dbf5119e72c89bf92e330816da8a48d4`; library `v0.2.0-alpha.1`, format 6.
Binary SHA-256: `1af4d3b32ea1aa24064d712362a6cf9ff615414807dfd181fcf16974f3a35cb9`.

Comparisons: **complete** (30/30 resource trials, 24/24 feature trials).

Feature harness: `f4aab14371aff8119df8d727c606fad3a4d7c59d`; only benchmark code differs from the tag.
go version go1.25.1 darwin/arm64
Apple M1 Pro
Logical CPUs: 10; memory: 16 GiB.

Three fresh processes per workload/engine; medians below. Resource tests use 1,000,000 keys, 16-byte keys, 100-byte values and 10 readers. Async writes; fixture payloads and settle/compaction peaks are included. All observations are retained.

| Profile | Workload | Engine | Peak RSS MiB | Peak OS footprint MiB | Load CPU seconds |
|---|---|---|---:|---:|---:|
| bounded64 | random | ours | 912.4 | 635.9 | 2.566 |
| bounded64 | random | pebble | 327.8 | 315.1 | 4.850 |
| bounded64 | random | sqlite-c | 935.1 | 1160.1 | 23.548 |
| bounded64 | random | sqlite-go | 1409.7 | 1525.3 | 43.169 |
| bounded64 | seq | ours | 764.2 | 517.0 | 1.615 |
| bounded64 | seq | pebble | 321.4 | 308.7 | 2.482 |
| bounded64 | seq | sqlite-c | 949.0 | 1159.8 | 13.623 |
| bounded64 | seq | sqlite-go | 1413.7 | 1525.4 | 16.109 |
| ours-default | random | ours | 323.3 | 302.8 | 1.474 |
| ours-default | seq | ours | 302.1 | 281.6 | 0.610 |

The bounded profile requests a 64 MiB cache for each engine; SQLite settings apply per connection, and Pebble also has a memtable. AptevaDB default uses its unlimited cache. CacheBytes is not a hard process-memory cap. These profiles have different allocation strategies, so they should not be described as equal total-memory budgets.

AptevaDB has a larger peak memory use than Pebble in the bounded profile. This release does not claim a universal speed/memory lead or prove no regression from a previous build. Background applications were active and recorded before every resource trial; no cold-OS-cache or isolated-machine claim is made.

Feature comparisons cover collections, batches, aggregation, joins, computed fields, computed collections and synchronous writes, retaining all three outer repetitions. Aggregation/join query timings retain the existing best-of-five inner timing; process CPU/resource measurements include all work. Native computed results and KV values are checked by the harness.

| Feature | Successful repetitions | Median process wall seconds |
|---|---:|---:|
| aggregate | 3 | 1.79 |
| batch | 3 | 24.80 |
| collections | 3 | 20.15 |
| computed-collections | 3 | 17.37 |
| computed-fields | 3 | 6.23 |
| durable | 3 | 75.62 |
| durable-batch | 3 | 4.35 |
| join | 3 | 2.62 |

## Resource workload timings

Median wall nanoseconds per operation across three fresh processes. Async writes; settle includes explicit maintenance and is reported separately in the JSON.

| Profile | Workload | Engine | Upsert ns/op | Hit ns/op | Miss ns/op | Parallel hit ns/op | Scan ns/op |
|---|---|---|---:|---:|---:|---:|---:|
| bounded64 | random | ours | 1030.1 | 741.6 | 475.4 | 493.8 | 48.0 |
| bounded64 | random | pebble | 1564.6 | 8542.6 | 8316.1 | 3984.8 | 99.0 |
| bounded64 | random | sqlite-c | 25506.6 | 3675.1 | 3873.2 | 2583.6 | 183.8 |
| bounded64 | random | sqlite-go | 45188.4 | 5099.5 | 4723.4 | 12154.7 | 159.3 |
| bounded64 | seq | ours | 481.9 | 984.1 | 903.7 | 516.8 | 47.9 |
| bounded64 | seq | pebble | 699.3 | 8500.2 | 8164.8 | 4440.8 | 98.0 |
| bounded64 | seq | sqlite-c | 14957.1 | 4143.4 | 3747.6 | 2185.5 | 160.9 |
| bounded64 | seq | sqlite-go | 20775.7 | 5160.1 | 4789.3 | 8852.1 | 145.8 |
| ours-default | random | ours | 950.4 | 744.7 | 478.6 | 93.6 | 46.4 |
| ours-default | seq | ours | 182.2 | 974.2 | 883.8 | 158.5 | 47.9 |

## Feature measurements

All three outer trials retained. Lower ns/op is better. Columns in raw aggregation/join outputs use milliseconds rounded to one decimal; zero indicates rounding, not zero query time. Their query timings select the best of five inner runs, unlike these feature medians.

| Feature | Workload | Engine | Phase | Median ns/op | Min–max |
|---|---|---|---|---:|---:|
| batch | random | ours-batch1000 | find-hit | 558.0 | 532.0–599.0 |
| batch | random | ours-batch1000 | find-hit-par10 | 72.0 | 68.0–85.0 |
| batch | random | ours-batch1000 | find-miss | 288.0 | 284.0–304.0 |
| batch | random | ours-batch1000 | insert | 556.0 | 545.0–592.0 |
| batch | random | ours-batch1000 | scan | 51.0 | 50.0–53.0 |
| batch | random | ours-batch1000 | settle | 502.0 | 444.0–539.0 |
| batch | seq | ours-batch1000 | find-hit | 650.0 | 605.0–845.0 |
| batch | seq | ours-batch1000 | find-hit-par10 | 88.0 | 80.0–118.0 |
| batch | seq | ours-batch1000 | find-miss | 589.0 | 504.0–672.0 |
| batch | seq | ours-batch1000 | insert | 151.0 | 140.0–225.0 |
| batch | seq | ours-batch1000 | scan | 48.0 | 46.0–48.0 |
| batch | seq | ours-batch1000 | settle | 405.0 | 385.0–445.0 |
| batch | random | pebble-batch1000 | find-hit | 2015.0 | 1992.0–2152.0 |
| batch | random | pebble-batch1000 | find-hit-par10 | 551.0 | 495.0–2041.0 |
| batch | random | pebble-batch1000 | find-miss | 1748.0 | 1720.0–2035.0 |
| batch | random | pebble-batch1000 | insert | 1099.0 | 1040.0–1163.0 |
| batch | random | pebble-batch1000 | scan | 51.0 | 50.0–127.0 |
| batch | random | pebble-batch1000 | settle | 3165.0 | 3075.0–3185.0 |
| batch | seq | pebble-batch1000 | find-hit | 2034.0 | 1747.0–2054.0 |
| batch | seq | pebble-batch1000 | find-hit-par10 | 490.0 | 483.0–583.0 |
| batch | seq | pebble-batch1000 | find-miss | 1801.0 | 1619.0–1818.0 |
| batch | seq | pebble-batch1000 | insert | 450.0 | 434.0–498.0 |
| batch | seq | pebble-batch1000 | scan | 53.0 | 46.0–53.0 |
| batch | seq | pebble-batch1000 | settle | 2674.0 | 2425.0–2909.0 |
| batch | random | sqlite-c-txn1000 | find-hit | 2532.0 | 2497.0–2535.0 |
| batch | random | sqlite-c-txn1000 | find-hit-par10 | 2386.0 | 2256.0–2949.0 |
| batch | random | sqlite-c-txn1000 | find-miss | 2296.0 | 2187.0–2929.0 |
| batch | random | sqlite-c-txn1000 | insert | 14597.0 | 13596.0–14666.0 |
| batch | random | sqlite-c-txn1000 | scan | 130.0 | 126.0–133.0 |
| batch | random | sqlite-c-txn1000 | settle | 25.0 | 23.0–27.0 |
| batch | seq | sqlite-c-txn1000 | find-hit | 2471.0 | 2455.0–2500.0 |
| batch | seq | sqlite-c-txn1000 | find-hit-par10 | 2098.0 | 2030.0–2255.0 |
| batch | seq | sqlite-c-txn1000 | find-miss | 2117.0 | 2088.0–2238.0 |
| batch | seq | sqlite-c-txn1000 | insert | 1580.0 | 1549.0–1658.0 |
| batch | seq | sqlite-c-txn1000 | scan | 113.0 | 113.0–115.0 |
| batch | seq | sqlite-c-txn1000 | settle | 41.0 | 35.0–58.0 |
| batch | random | sqlite-go-txn1000 | find-hit | 4081.0 | 3248.0–4662.0 |
| batch | random | sqlite-go-txn1000 | find-hit-par10 | 6904.0 | 6189.0–7855.0 |
| batch | random | sqlite-go-txn1000 | find-miss | 3172.0 | 3138.0–3636.0 |
| batch | random | sqlite-go-txn1000 | insert | 17142.0 | 15830.0–21244.0 |
| batch | random | sqlite-go-txn1000 | scan | 101.0 | 97.0–105.0 |
| batch | random | sqlite-go-txn1000 | settle | 60.0 | 21.0–105.0 |
| batch | seq | sqlite-go-txn1000 | find-hit | 3567.0 | 3386.0–3808.0 |
| batch | seq | sqlite-go-txn1000 | find-hit-par10 | 6874.0 | 6677.0–7011.0 |
| batch | seq | sqlite-go-txn1000 | find-miss | 3199.0 | 3157.0–3206.0 |
| batch | seq | sqlite-go-txn1000 | insert | 2162.0 | 1950.0–2174.0 |
| batch | seq | sqlite-go-txn1000 | scan | 92.0 | 89.0–94.0 |
| batch | seq | sqlite-go-txn1000 | settle | 27.0 | 22.0–72.0 |
| collections | random | ours-coll4 | find-hit | 486.0 | 467.0–581.0 |
| collections | random | ours-coll4 | find-hit-par10 | 89.0 | 75.0–112.0 |
| collections | random | ours-coll4 | find-miss | 289.0 | 289.0–324.0 |
| collections | random | ours-coll4 | insert | 601.0 | 592.0–639.0 |
| collections | random | ours-coll4 | scan | 52.0 | 50.0–54.0 |
| collections | random | ours-coll4 | settle | 521.0 | 394.0–2422.0 |
| collections | seq | ours-coll4 | find-hit | 622.0 | 616.0–740.0 |
| collections | seq | ours-coll4 | find-hit-par10 | 111.0 | 94.0–123.0 |
| collections | seq | ours-coll4 | find-miss | 608.0 | 501.0–610.0 |
| collections | seq | ours-coll4 | insert | 315.0 | 312.0–324.0 |
| collections | seq | ours-coll4 | scan | 49.0 | 49.0–50.0 |
| collections | seq | ours-coll4 | settle | 526.0 | 502.0–596.0 |
| collections | random | pebble-coll4 | find-hit | 2143.0 | 2033.0–2388.0 |
| collections | random | pebble-coll4 | find-hit-par10 | 537.0 | 531.0–557.0 |
| collections | random | pebble-coll4 | find-miss | 1767.0 | 1761.0–1793.0 |
| collections | random | pebble-coll4 | insert | 1663.0 | 1505.0–1734.0 |
| collections | random | pebble-coll4 | scan | 49.0 | 48.0–51.0 |
| collections | random | pebble-coll4 | settle | 4442.0 | 3742.0–7726.0 |
| collections | seq | pebble-coll4 | find-hit | 2146.0 | 2033.0–2300.0 |
| collections | seq | pebble-coll4 | find-hit-par10 | 540.0 | 483.0–615.0 |
| collections | seq | pebble-coll4 | find-miss | 1785.0 | 1778.0–1898.0 |
| collections | seq | pebble-coll4 | insert | 1011.0 | 941.0–1072.0 |
| collections | seq | pebble-coll4 | scan | 52.0 | 50.0–53.0 |
| collections | seq | pebble-coll4 | settle | 3553.0 | 2964.0–4340.0 |
| collections | random | sqlite-c-tables4 | find-hit | 2634.0 | 2545.0–2925.0 |
| collections | random | sqlite-c-tables4 | find-hit-par10 | 2184.0 | 2128.0–2271.0 |
| collections | random | sqlite-c-tables4 | find-miss | 2262.0 | 2168.0–2291.0 |
| collections | random | sqlite-c-tables4 | insert | 22246.0 | 21739.0–37947.0 |
| collections | random | sqlite-c-tables4 | scan | 126.0 | 125.0–126.0 |
| collections | random | sqlite-c-tables4 | settle | 32.0 | 28.0–41.0 |
| collections | seq | sqlite-c-tables4 | find-hit | 2493.0 | 2484.0–2702.0 |
| collections | seq | sqlite-c-tables4 | find-hit-par10 | 2205.0 | 2144.0–2371.0 |
| collections | seq | sqlite-c-tables4 | find-miss | 2204.0 | 2170.0–2674.0 |
| collections | seq | sqlite-c-tables4 | insert | 13819.0 | 13051.0–14424.0 |
| collections | seq | sqlite-c-tables4 | scan | 122.0 | 119.0–123.0 |
| collections | seq | sqlite-c-tables4 | settle | 13.0 | 11.0–14.0 |
| computed-collections | fixture | ours-native | delete | 1118.0 | 928.2–1144.7 |
| computed-collections | fixture | ours-native | disk | 78208.0 | 68292.0–86500.0 |
| computed-collections | fixture | ours-native | insert | 1741.7 | 1686.2–1778.3 |
| computed-collections | fixture | ours-native | point-par10 | 19.6 | 18.4–25.0 |
| computed-collections | fixture | ours-native | point-par32 | 23.1 | 19.5–24.4 |
| computed-collections | fixture | ours-native | point-query | 128.4 | 127.5–130.3 |
| computed-collections | fixture | ours-native | query-all | 68174.8 | 67081.7–69546.8 |
| computed-collections | fixture | ours-native | upsert | 2098.4 | 2068.3–2149.1 |
| computed-collections | fixture | ours-query | backfill | 488.4 | 363.1–776.1 |
| computed-collections | fixture | ours-query | delete | 258.7 | 235.1–331.6 |
| computed-collections | fixture | ours-query | disk | 133125.0 | 129958.0–148083.0 |
| computed-collections | fixture | ours-query | insert | 401.9 | 383.2–621.5 |
| computed-collections | fixture | ours-query | point-query | 15998.7 | 15517.6–25388.8 |
| computed-collections | fixture | ours-query | projection-backfill | 487.8 | 281.7–723.1 |
| computed-collections | fixture | ours-query | query-all | 1878454.3 | 1449732.1–2260468.3 |
| computed-collections | fixture | ours-query | upsert | 214.3 | 198.2–233.9 |
| computed-collections | fixture | pebble-manual | delete | 1651.6 | 1513.6–1716.3 |
| computed-collections | fixture | pebble-manual | disk | 123666.0 | 122125.0–166875.0 |
| computed-collections | fixture | pebble-manual | insert | 4502.1 | 4482.0–4624.6 |
| computed-collections | fixture | pebble-manual | point-par10 | 798.3 | 785.9–1127.3 |
| computed-collections | fixture | pebble-manual | point-par32 | 836.8 | 823.3–857.7 |
| computed-collections | fixture | pebble-manual | point-query | 1288.5 | 1260.6–1297.8 |
| computed-collections | fixture | pebble-manual | query-all | 95544.9 | 91987.5–97185.1 |
| computed-collections | fixture | pebble-manual | upsert | 3734.0 | 3680.6–4191.3 |
| computed-collections | fixture | pebble-query | delete | 443.8 | 419.9–544.9 |
| computed-collections | fixture | pebble-query | disk | 110667.0 | 100625.0–113709.0 |
| computed-collections | fixture | pebble-query | insert | 1665.4 | 1654.3–1793.8 |
| computed-collections | fixture | pebble-query | point-query | 1381785.0 | 1360721.7–1394743.3 |
| computed-collections | fixture | pebble-query | query-all | 1747740.1 | 1738763.9–1751799.6 |
| computed-collections | fixture | pebble-query | upsert | 991.0 | 949.6–1067.3 |
| computed-collections | fixture | sqlite-c-native | delete | 3405.0 | 3308.7–3628.4 |
| computed-collections | fixture | sqlite-c-native | disk | 84916.0 | 77000.0–86750.0 |
| computed-collections | fixture | sqlite-c-native | insert | 3272.2 | 3253.4–3499.3 |
| computed-collections | fixture | sqlite-c-native | point-par10 | 1116.0 | 1057.0–1173.0 |
| computed-collections | fixture | sqlite-c-native | point-par32 | 1236.9 | 1233.4–1256.4 |
| computed-collections | fixture | sqlite-c-native | point-query | 1429.5 | 1418.6–1642.8 |
| computed-collections | fixture | sqlite-c-native | query-all | 181973.8 | 178712.6–185154.3 |
| computed-collections | fixture | sqlite-c-native | upsert | 4308.6 | 4181.2–5121.1 |
| computed-collections | fixture | sqlite-c-query | delete | 1478.4 | 1411.7–1714.6 |
| computed-collections | fixture | sqlite-c-query | disk | 99000.0 | 64375.0–101334.0 |
| computed-collections | fixture | sqlite-c-query | insert | 1977.7 | 1918.1–2151.2 |
| computed-collections | fixture | sqlite-c-query | point-query | 2691.8 | 2451.2–2805.6 |
| computed-collections | fixture | sqlite-c-query | query-all | 827829.3 | 811545.6–938470.2 |
| computed-collections | fixture | sqlite-c-query | upsert | 2185.2 | 2022.7–2393.8 |
| computed-collections | fixture | sqlite-go-native | delete | 3967.0 | 3923.6–4841.8 |
| computed-collections | fixture | sqlite-go-native | disk | 120166.0 | 93542.0–127542.0 |
| computed-collections | fixture | sqlite-go-native | insert | 4114.4 | 3945.6–4183.3 |
| computed-collections | fixture | sqlite-go-native | point-par10 | 2783.2 | 2640.0–2879.9 |
| computed-collections | fixture | sqlite-go-native | point-par32 | 3188.9 | 3144.2–3202.9 |
| computed-collections | fixture | sqlite-go-native | point-query | 2005.7 | 1960.7–2115.1 |
| computed-collections | fixture | sqlite-go-native | query-all | 100831.5 | 98068.0–127154.5 |
| computed-collections | fixture | sqlite-go-native | upsert | 5763.3 | 5396.6–5885.0 |
| computed-collections | fixture | sqlite-go-query | delete | 1827.8 | 1640.8–1852.9 |
| computed-collections | fixture | sqlite-go-query | disk | 90250.0 | 71084.0–108500.0 |
| computed-collections | fixture | sqlite-go-query | insert | 2648.9 | 2577.1–4405.0 |
| computed-collections | fixture | sqlite-go-query | point-query | 4246.9 | 4233.4–4645.8 |
| computed-collections | fixture | sqlite-go-query | query-all | 1359394.8 | 1315908.7–1417720.2 |
| computed-collections | fixture | sqlite-go-query | upsert | 2485.5 | 2465.2–2638.4 |
| computed-fields | fixture | ours-manual | index-build+verify | 1575.1 | 1502.9–1782.0 |
| computed-fields | fixture | ours-manual | indexed-filter | 36614.6 | 35125.0–43414.6 |
| computed-fields | fixture | ours-manual | insert | 580.6 | 514.5–623.8 |
| computed-fields | fixture | ours-manual | read-derived | 271.3 | 243.7–318.4 |
| computed-fields | fixture | ours-manual | read-derived-10 | 45.6 | 38.9–53.3 |
| computed-fields | fixture | ours-manual | read-derived-32 | 38.3 | 34.7–46.3 |
| computed-fields | fixture | ours-manual | upsert | 340.6 | 333.4–348.5 |
| computed-fields | fixture | ours-native | index-build+verify | 1498.2 | 1448.8–1702.4 |
| computed-fields | fixture | ours-native | indexed-filter | 37556.2 | 36064.6–42687.5 |
| computed-fields | fixture | ours-native | insert | 939.5 | 902.9–1025.3 |
| computed-fields | fixture | ours-native | read-derived | 255.7 | 255.2–274.4 |
| computed-fields | fixture | ours-native | read-derived-10 | 44.5 | 38.7–55.8 |
| computed-fields | fixture | ours-native | read-derived-32 | 35.8 | 32.9–45.2 |
| computed-fields | fixture | ours-native | upsert | 765.8 | 721.1–785.0 |
| computed-fields | fixture | ours-readtime | derive-filter-scan | 3253777.0 | 3250160.5–3385733.4 |
| computed-fields | fixture | ours-readtime | insert | 389.0 | 386.0–393.7 |
| computed-fields | fixture | ours-readtime | read-derived | 291.9 | 261.0–298.0 |
| computed-fields | fixture | ours-readtime | read-derived-10 | 51.5 | 39.5–66.6 |
| computed-fields | fixture | ours-readtime | read-derived-32 | 45.2 | 43.5–49.8 |
| computed-fields | fixture | ours-readtime | upsert | 239.7 | 212.0–249.2 |
| computed-fields | fixture | pebble-manual | insert | 1566.4 | 1405.5–1706.5 |
| computed-fields | fixture | pebble-manual | read-derived | 1570.6 | 1359.7–1735.7 |
| computed-fields | fixture | pebble-manual | read-derived-10 | 792.9 | 710.2–797.9 |
| computed-fields | fixture | pebble-manual | read-derived-32 | 776.3 | 721.8–786.0 |
| computed-fields | fixture | pebble-manual | upsert | 903.9 | 881.3–920.9 |
| computed-fields | fixture | pebble-readtime | insert | 1178.1 | 1142.6–1637.4 |
| computed-fields | fixture | pebble-readtime | read-derived | 1373.1 | 1365.2–1543.0 |
| computed-fields | fixture | pebble-readtime | read-derived-10 | 743.7 | 718.3–754.4 |
| computed-fields | fixture | pebble-readtime | read-derived-32 | 786.3 | 747.7–919.8 |
| computed-fields | fixture | pebble-readtime | upsert | 420.8 | 419.6–1321.8 |
| computed-fields | fixture | sqlite-c | index-build | 624.4 | 444.8–635.6 |
| computed-fields | fixture | sqlite-c | indexed-filter | 32818.8 | 26252.0–36133.3 |
| computed-fields | fixture | sqlite-c | insert | 4872.0 | 4839.9–5030.3 |
| computed-fields | fixture | sqlite-c | read-derived | 2038.5 | 1821.8–2131.2 |
| computed-fields | fixture | sqlite-c | upsert | 8735.1 | 7058.7–8962.1 |
| computed-fields | fixture | sqlite-go | index-build | 712.6 | 672.0–713.6 |
| computed-fields | fixture | sqlite-go | indexed-filter | 49762.5 | 46989.6–49845.8 |
| computed-fields | fixture | sqlite-go | insert | 5439.1 | 4900.2–6217.2 |
| computed-fields | fixture | sqlite-go | read-derived | 2742.9 | 2447.0–2814.2 |
| computed-fields | fixture | sqlite-go | upsert | 7659.2 | 6974.3–8000.3 |
| durable | random | ours-sync | insert | 4312189.0 | 4157507.0–4357557.0 |
| durable | random | ours-sync | insert-par10 | 505341.0 | 501759.0–528824.0 |
| durable | random | ours-sync | insert-par50 | 113136.0 | 93887.0–121813.0 |
| durable | seq | ours-sync | insert | 4558788.0 | 4469877.0–4783864.0 |
| durable | seq | ours-sync | insert-par10 | 487784.0 | 473879.0–513111.0 |
| durable | seq | ours-sync | insert-par50 | 96092.0 | 86993.0–113867.0 |
| durable | random | pebble-sync | insert | 4515467.0 | 4417400.0–4836612.0 |
| durable | random | pebble-sync | insert-par10 | 906322.0 | 902974.0–1004253.0 |
| durable | random | pebble-sync | insert-par50 | 190603.0 | 172973.0–200093.0 |
| durable | seq | pebble-sync | insert | 4640833.0 | 4241739.0–5677604.0 |
| durable | seq | pebble-sync | insert-par10 | 861911.0 | 861497.0–900888.0 |
| durable | seq | pebble-sync | insert-par50 | 180076.0 | 174967.0–197947.0 |
| durable | random | sqlite-c-sync | insert | 4730909.0 | 4576830.0–4909860.0 |
| durable | random | sqlite-c-sync | insert-par10 | 4348003.0 | 4311956.0–5000909.0 |
| durable | random | sqlite-c-sync | insert-par50 | 4365037.0 | 4326792.0–4598733.0 |
| durable | seq | sqlite-c-sync | insert | 4647813.0 | 4462918.0–4725879.0 |
| durable | seq | sqlite-c-sync | insert-par10 | 4024908.0 | 3980754.0–4246812.0 |
| durable | seq | sqlite-c-sync | insert-par50 | 4137715.0 | 3860156.0–4574945.0 |
| durable | random | sqlite-go-sync | insert | 4813979.0 | 4773757.0–4924070.0 |
| durable | random | sqlite-go-sync | insert-par10 | 3963630.0 | 3856654.0–4729542.0 |
| durable | random | sqlite-go-sync | insert-par50 | 4487840.0 | 4057969.0–4819221.0 |
| durable | seq | sqlite-go-sync | insert | 4485819.0 | 4296913.0–4573863.0 |
| durable | seq | sqlite-go-sync | insert-par10 | 4251871.0 | 4010959.0–4346929.0 |
| durable | seq | sqlite-go-sync | insert-par50 | 4538947.0 | 4080837.0–5012244.0 |
| durable-batch | random | ours-sync-batch1000 | insert | 5530.0 | 4977.0–5543.0 |
| durable-batch | random | ours-sync-batch1000 | insert-par10 | 5808.0 | 5024.0–5829.0 |
| durable-batch | random | ours-sync-batch1000 | insert-par50 | 5682.0 | 5276.0–6281.0 |
| durable-batch | seq | ours-sync-batch1000 | insert | 5327.0 | 5239.0–5339.0 |
| durable-batch | seq | ours-sync-batch1000 | insert-par10 | 5229.0 | 4977.0–5675.0 |
| durable-batch | seq | ours-sync-batch1000 | insert-par50 | 6429.0 | 5124.0–7774.0 |
| durable-batch | random | pebble-sync-batch1000 | insert | 7497.0 | 6195.0–7544.0 |
| durable-batch | random | pebble-sync-batch1000 | insert-par10 | 5319.0 | 4977.0–5421.0 |
| durable-batch | random | pebble-sync-batch1000 | insert-par50 | 5723.0 | 5526.0–6504.0 |
| durable-batch | seq | pebble-sync-batch1000 | insert | 7446.0 | 7058.0–8746.0 |
| durable-batch | seq | pebble-sync-batch1000 | insert-par10 | 5464.0 | 4937.0–6552.0 |
| durable-batch | seq | pebble-sync-batch1000 | insert-par50 | 5880.0 | 5619.0–8257.0 |
| durable-batch | random | sqlite-c-sync-txn1000 | insert | 15052.0 | 14792.0–17805.0 |
| durable-batch | random | sqlite-c-sync-txn1000 | insert-par10 | 28232.0 | 23881.0–31131.0 |
| durable-batch | random | sqlite-c-sync-txn1000 | insert-par50 | 39784.0 | 31739.0–41479.0 |
| durable-batch | seq | sqlite-c-sync-txn1000 | insert | 6093.0 | 5995.0–10186.0 |
| durable-batch | seq | sqlite-c-sync-txn1000 | insert-par10 | 15490.0 | 14843.0–21894.0 |
| durable-batch | seq | sqlite-c-sync-txn1000 | insert-par50 | 25037.0 | 21476.0–36923.0 |
