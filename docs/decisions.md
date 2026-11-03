# Decisions

## Serialize appends on PostgreSQL to keep sequence order and visibility order identical

Status: accepted

### Context

`sequence_number` is allocated at insert time, but an event only becomes visible at commit. Two concurrent appends can commit out of sequence order, so a reader can see #102 before #101. This caused two bugs on PostgreSQL:

- **Subscribers lose events:** a subscriber that checkpointed at #102 never receives #101.
- **Decisions ignore events:** a decision read sees #102 but not #101, and the subsequent append with `after: 102` only checks events above #102, so it succeeds although #101 matches. The read happens outside of the append's transaction, so `SERIALIZABLE` can't detect this.

### Decision

`append()` on PostgreSQL holds a transaction-level advisory lock until after the commit, so that a lower sequence number is always visible before a higher one is allocated:

```sql
BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtextextended('dcb_<table>', 0));
INSERT ...;
COMMIT;
```

| Topic | Decision |
|---|---|
| Availability | Always on, without opt-out. An opt-out would bring back both bugs |
| Reads | Unchanged, including `lastPosition()` of `wwwision/subscription-engine-dcb-adapter`: any event committed later gets a higher position than the highest visible one |
| Schema | Unchanged: no new column, index or migration |
| Isolation level | `READ COMMITTED`, because the lock provides the mutual exclusion. A lock statement inside a `SERIALIZABLE` transaction would take the snapshot before waiting for the lock, so the condition check would miss the event committed in the meantime |
| Lock scope | Per database and table name. A hash collision only causes additional waiting |
| Lock timeout | 30 seconds like the `GET_LOCK` of the MySQL path. A timeout results in a `RuntimeException` and isn't retried |
| MySQL/MariaDB | Unchanged, the existing `GET_LOCK` already covers the commit. The ordering tests pass without changes |
| SQLite | Unchanged, because it has a single writer. The ordering tests pass without changes |
| Guarantee | Only covers appends through `DoctrineEventStore` on a single primary. Other writers have to take the same lock (see README). `GET_LOCK` doesn't protect multi-primary setups like Galera |
| Upgrade | Stop old writers before new ones start appending. Old writers skip the lock, so during an overlap gaps remain possible and append conditions can be violated. Events that subscribers skipped in the past are not recovered |

### Consequences

Appends per table are limited by the duration of one `INSERT` and `COMMIT`. The limit applies per append rather than per event, so batching events into one append helps.

Measurements with PostgreSQL 17 in local Docker, 16 PHP processes, 8 seconds per run. These are not production numbers: fsync took about 82µs here, and network round trips are close to zero. Slower storage and network lower the limit.

| `DoctrineEventStore::append()` | Without lock | With lock |
|---|---|---|
| Unconditional | ~4000/s | ~810/s |
| Conditional | 573/s and 1327/s, with retry failures in the slower run | ~590/s, no failures |

### Rejected alternatives

**`tx_id xid8` column with a `tx_id < pg_snapshot_xmin(pg_current_snapshot())` read filter:** sequence numbers and transaction IDs are allocated independently, so their orders can differ. A stress test with a tailing reader still missed 21 of 248,064 events and found 1137 adjacent inversions. It would also stall subscribers whenever any write transaction in the cluster runs for long.

**Watermark with a composite checkpoint `(tx_id, sequence_number)`:** should keep parallel writes, but breaks `SequencePosition`, the core package, the adapter and stored checkpoints.

**Watermark with a lock that is released before the commit:** barely faster in a raw SQL benchmark (5.1k vs. 4.1k tps), keeps the stalls of the watermark, and condition checks could miss appends that are about to commit.

**Session-level advisory lock before `BEGIN SERIALIZABLE`:** safer while old writers are still running, but not safe with PgBouncer in transaction mode, where lock and unlock can end up on different backends. This risk would be permanent, while the upgrade overlap is not.

**`LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE`:** also blocks other writers, but conflicts with `VACUUM` and `ANALYZE`.

**`synchronous_commit = off` to shorten the lock duration:** a crash could lose acknowledged events.

## Read events by tag partition

Status: accepted

### Context

To share one projection between N workers, each worker should only read the events of the entities it owns. Filtering in PHP means every worker reads and deserializes the full log. A partition has to be selected in SQL, but the concept is outside of the DCB specification, so it can't be part of the `Query` of `wwwision/dcb-eventstore`.

### Decision

`DoctrineEventStore::readPartition()` returns the events with a tag that starts with a given prefix and hashes into partition `p` of `n`. `DoctrineEventStore::head()` returns the position to read up to:

```php
$partition = TagPartition::create(prefix: 'cart:', partitions: 8, partition: 3);
$events = $eventStore->readPartition($partition, Query::all(), ReadOptions::create(from: 1001, limit: 1000), until: $eventStore->head());
```

| Topic | Decision |
|---|---|
| Source | Tags. They already identify the entities on all existing events and require no change to writers. Removing a tag from new events is a breaking change for partitioned readers |
| Hash | First 32 bits of the SHA-256 of the whole tag as an unsigned integer, modulo `n`. This can't change without rebuilding all partitioned projections. Unsigned is required, a signed value leads to negative remainders |
| Several matching tags | The event is returned by every partition that owns one of them, so partitions can overlap. Otherwise the owner of the other tag would silently miss it. A worker uses `TagPartition::contains()` to determine the tags it owns |
| No matching tag | The event isn't returned by any partition |
| Prefix | Literal and case-sensitive. `LIKE` is not an option because `_` is valid in tags, and `my_cart:%` would match `myXcart:1`. A trailing colon isn't enforced, so `cart` matches `cartoon:1` |
| Query | ANDed with the partition. `onlyLastEvent` is rejected, because the maximum is determined before the partition filter applies and the result would look valid but be mostly empty. Without `onlyLastEvent`, the query items can be part of the `WHERE` clause instead of the derived table of `read()`, which MySQL and MariaDB materialize with all matching events regardless of the position and limit |
| `until` | Inclusive and forward-only. Without it, a worker only learns the positions of hits, so a worker with a rare prefix rescans everything since its last hit on every poll, regardless of `limit` |
| Checkpoint | The last event's position if a batch reached `limit`, otherwise `until`. This is safe because a lower position can't become visible after a higher one (see above) |
| `head()` | `MAX(sequence_number)`, available on all platforms. It must never read the sequence or `AUTO_INCREMENT` counter: those are ahead of the committed events while an append is in flight, so a worker would checkpoint past events it hasn't seen |
| PHP hash | Public as `TagPartition::contains()`, because workers need it. Tests freeze the contract with hard-coded values and compare SQL and PHP on every platform |
| SQL | Inline, without a function created by `setup()`. An `IMMUTABLE` function would only be required for an expression index. No schema change or migration |
| MySQL/MariaDB SQL | A scalar `(SELECT 1 FROM JSON_TABLE(...) ... LIMIT 1) IS NOT NULL` instead of `EXISTS`. MySQL 8.0 and 8.4 turn `EXISTS` into a semi-join that evaluates `JSON_TABLE` before the table it depends on, and silently return no rows for tables with more than one row. MariaDB is not affected |
| Index | None. Reads walk the primary key. An index would tie the schema to specific prefixes |
| Platforms | PostgreSQL and MySQL/MariaDB. SQLite has no built-in hash function and multiple workers on one database file are unrealistic, so `readPartition()` throws there |
| Rescaling | Documented, not enforced, because checkpoints belong to the caller. Doubling `n` splits partition `k` into `k` and `k + n`. Any `n` can be doubled, it doesn't have to be a power of two |
| Subscription engine | Out of scope. `wwwision/subscription-engine` only advances to the last handled event and its adapter interface has no upper bound, so `until` can't be used through it. An adapter based on `readPartition()` would work, with the rescans described above. Avoiding them requires a change to the engine |

### Consequences

- Partitions can overlap, which is unsuitable for side effects like sending one email per event.
- Catching up costs a primary key scan per worker, as the GIN index on tags can't serve a prefix or hash.
- Changing `n` to anything but double or half requires a rebuild, because most tags move.
- There are two ways to apply a `Query`. `read()` and append conditions still use the derived table, and have the same cost on MySQL and MariaDB.

Measurements with PHP 8.5 through `readPartition()` and local Docker, best of 3 runs, one client, `partitions: 8`. 41% of the events have a `cart:` tag, 1% have two. These are not production numbers.

| | PostgreSQL 17, 5M events | PostgreSQL 17, 1M | MySQL 8.4, 1M | MariaDB 11.4, 1M |
|---|---|---|---|---|
| `read()` of all events, for comparison | 25.5s | 5.0s | 4.7s | 4.7s |
| Whole partition | 7.3s | 1.5s | 1.5s | 1.5s |
| Batch of 1000 from the start | 29ms | 29ms | 28ms | 26ms |
| Batch of 1000 with a type `Query` | 20ms | 20ms | 26ms | 23ms |
| Last 50,000 positions with `until` | 76ms | 76ms | 69ms | 68ms |
| Prefix without matches, `limit: 1000` | 3.8s | 0.7s | 0.9s | 1.2s |
| `head()` | 0.5ms | 0.5ms | 0.3ms | 0.4ms |

### Rejected alternatives

**Single partition key in the event metadata, one partition per event:** simpler SQL and disjoint partitions, but one key can't serve a by-course and a by-student projection of the same event, events concerning two entities reach only one of their workers, existing events have no key, and nothing enforces that writers set it.

**`hashtext()`:** the fastest option, but undocumented without a stability guarantee, hard to reproduce in PHP, and signed.

**MD5:** about the same cost as SHA-256, but `md5()` is expected to fail on PostgreSQL built with OpenSSL in FIPS mode. The hash can't be changed later, so it should not depend on that.

**Canonical tag per event (for example the smallest match):** partitions would be disjoint, but the owner of any other tag would silently miss the event.

**Extending `ReadOptions`:** `final` with a private constructor in the core package.

**Decorator around `EventStore`:** requires private query building of the store, and `EventStore::read()` has no parameter for `until`.

**Derived table of `read()` for the `Query`:** avoids a second way to apply a `Query`, but a batch of 1000 with a type `Query` took 1096ms on MySQL 8.4 and 138ms on MariaDB 11.4 with 1 million events.

**Separate `PartitionedReader`:** would require moving query building and row mapping into a shared class for a single additional caller.

**Tag prefix in the DCB `Query` (`cart:*`):** doesn't select a partition by itself. As `Query` is also used for append conditions, it would allow full scans while holding the append lock. It also requires changes to the core package and all of its adapters. It might still be useful as a separate proposal.

**Expression GIN index over virtual buckets:** in a test with 5 million events it wasn't used by PostgreSQL for a batch at `n = 8`, and it ties the schema to specific prefixes.
