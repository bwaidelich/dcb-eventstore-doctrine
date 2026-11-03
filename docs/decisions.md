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
