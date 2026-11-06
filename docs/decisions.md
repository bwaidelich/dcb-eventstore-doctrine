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
| Guarantee | Only covers appends through `DoctrineEventStore` on a single primary. Other writers have to take the same lock (see [Event ordering](advanced.md#event-ordering)). `GET_LOCK` doesn't protect multi-primary setups like Galera |
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

## Apply a query for one event type without a derived table

Status: accepted

### Context

`read()` joins a derived table that contains all events matching the query, regardless of the requested position and limit. A query for a common event type pays for all events of that type, even for `limit: 1`.

### Decision

`read()` applies a `Query` in the `WHERE` clause if it consists of one item with exactly one event type, no tags and without `onlyLastEvent`. The database can walk the index on `(type, sequence_number)` in the requested order. All other queries and append conditions keep the derived table.

Measurements with raw SQL in local Docker, best of 2 runs, for a query with one common event type:

| | PostgreSQL 17, 5M events | MySQL 8.4, 1M | MariaDB 11.4, 1M |
|---|---|---|---|
| `limit: 1000` from the start | 2ms -> 2ms | 871ms -> 3ms | 130ms -> 4ms |
| `limit: 1000` from 90% of the log | 188ms -> 2ms | 398ms -> 2ms | 13ms -> 3ms |
| Backwards, `limit: 1` | 329ms -> 1ms | 789ms -> 1ms | 119ms -> 1ms |

### Rejected alternatives

**`WHERE` clause for all queries without `onlyLastEvent`:** much faster with evenly distributed events, but without an index that provides the order, databases walk the primary key if they expect a match soon. If there is no match, or the matching events are clustered at the other end of the log, this scans the table:

| Query | Engine, events | Derived table | `WHERE` |
|---|---|---|---|
| Tag without matches, backwards, `limit: 1` | PostgreSQL 17, 5M | 0.5ms | 1143ms |
| Two types only used recently, `limit: 1` | PostgreSQL 17, 5M | 1ms | 1011ms |
| Two types only used early, backwards, `limit: 1` | MySQL 8.4, 1M | 7ms | 451ms |
| Two types only used early, backwards, `limit: 1` | MariaDB 11.4, 1M | 2ms | 969ms |

Queries like this are typical, for example to check for the last event with a tag, and the cost grows with the log.

## Wait for new events by polling

Status: accepted

### Context

An idle worker has to find out when there is something new to read. Without help from the event store, every worker implements its own loop of sleeping and reading.

### Decision

`DoctrineEventStore::createWaiter()` returns an `EventStoreWaiter` that blocks until there might be new events:

```php
$waiter = $eventStore->createWaiter($dedicatedConnection);
while (true) {
    $checkpoint = catchUp($eventStore, $checkpoint);
    $waiter->waitForNewEvents(30.0);
}
```

| Topic | Decision |
|---|---|
| Mechanism | Calls `head()` once per interval (default: 1 second) with ±20% jitter per sleep, so that workers don't query at the same time. `MAX(sequence_number)` reads one entry of the primary key and costs about 0.5ms regardless of the size of the table |
| Result | `true`: there might be new events, read now. `false`: the head didn't change since the last check, including if a signal ended the wait early |
| First call | Determines the head and returns `true` without waiting, so that a worker catches up after the waiter became active |
| Comparison | For inequality, because the head can move backwards after a failover |
| Connection | Dedicated and provided by the caller, so that it is never inside a transaction of the worker. A connection inside a transaction is rejected |
| Lost connections | Any exception while checking the head closes the connection and reconnects once within the same call, followed by `true`. A failing reconnect results in a `RuntimeException`, the next call tries again. There is no backoff, that is up to the worker |
| Timeout | Negative values are rejected, `0.0` doesn't block |
| Signals | End the wait early without resuming it, so that a worker can react to a shutdown right away |
| Interval | Fixed and configurable. An adaptive interval can be added later without a breaking change |
| Location | This package, as the core package has no concept of a head or waiting. It could move there later |

### Consequences

- New events are noticed with a latency of up to the interval.
- Every idle worker queries the database once per interval.

### Rejected alternatives

**`subscribe()` returning an endless stream of events:** it would have to be built on the waiter anyway, as the database is the only component shared by writers and workers. An endless stream hides when a worker has caught up, so a handler can't commit once per batch without an additional marker or timer. A portable `subscribe()` would rather belong to a subscription engine, which could map it to the native streams of other event stores.

**`LISTEN`/`NOTIFY` on PostgreSQL:** deferred, see [the decision about deferring it](#defer-partitioned-reads-and-notifications).

## Don't expose exceptions of the database library

Status: accepted

### Context

`append()` and `setup()` already converted most exceptions of DBAL, but `read()`, `readPartition()`, `head()` and the iteration over read events didn't. Callers had to know about the adapter's library, although `EventStore` doesn't mention it.

### Decision

Public methods of `DoctrineEventStore` throw a `RuntimeException` with the DBAL or JSON exception as `$previous`.

| Topic | Decision |
|---|---|
| Exception class | `RuntimeException`, like the existing conversions. Nothing has to tell these failures apart so far, and a subclass can be added later without a breaking change |
| Lazy reads | Events are fetched while iterating, so the generator converts exceptions as well |
| JSON | A `JsonException` is converted the same way. It can be caused by metadata that can't be encoded, or by tags in a row that can't be decoded |
| Failing rollback | Ignored, so that it can't replace the exception that caused it. The transaction ends with the connection |
| Waiters | Catch `RuntimeException` from `head()` to reconnect |

### Consequences

- Breaking for callers that catch a specific DBAL exception from `read()`, `readPartition()`, `head()` or while iterating. They have to inspect `getPrevious()` instead.

### Rejected alternatives

**Dedicated exception class:** no caller needs it yet.

## Keep one package for all database platforms

Status: accepted

### Context

Some behavior differs per platform, for example the locking of appends. Dedicated packages would express that in their types instead of at runtime.

### Decision

PostgreSQL, MySQL/MariaDB and SQLite stay in one package, and the platform checks stay inline in `DoctrineEventStore`.

| Topic | Decision |
|---|---|
| Size of the differences | 16 platform checks in 580 lines of `DoctrineEventStore`, the rest is shared |
| Dependencies | Would be almost identical per package, so a split wouldn't save installing anything |
| Platform strategy class | Not extracted. It would refactor the append locking without a need as long as there is one package |
| Revisit | If a platform needs a different schema or append strategy |

### Consequences

- Behavior can differ depending on the platform, which only the documentation states.

### Rejected alternatives

**One package per platform:** requires 3 packages and probably a shared one, each with its own release and CI.

## Keep Doctrine DBAL

Status: accepted

### Context

DBAL hardly appears in the public API anymore: `create()` and `createWaiter()` accept a `Connection`, and exceptions are converted. `QueryBuilder` mostly assembles strings, and `setup()` has to work around the comparator to keep the GIN index. Using PDO directly would remove a dependency and the need to support several major versions of it.

### Decision

The package keeps requiring `doctrine/dbal` `^3.6 || ^4`.

| Topic | Decision |
|---|---|
| Replaced functionality | Schema comparison, expansion of array parameters and deadlock detection would have to be implemented for 3 platforms |
| Reconnecting | The documented recovery calls `close()` on the connection, and DBAL reconnects with the next statement. PDO can't do that, so `create()` would need a connection factory |
| Consumers | Other packages of the DCB ecosystem use DBAL themselves |
| Name | Dropping DBAL requires renaming the package |
| Timing | The upgrade to 7.0 already requires stopping all writers on PostgreSQL, so it shouldn't include a rewrite as well |
| Configuration | `connection`, `platform`, the parameter name methods and the platform checks of `DoctrineEventStoreConfiguration` are `@internal`, so that a later change doesn't have to treat them as public API |
| Revisit | Can be a separate major version: the public DBAL surface is limited to the `Connection` parameters |

### Rejected alternatives

**PDO in 7.0:** see above.

**Dropping DBAL 3:** 3.x support was added back on purpose, and removing it would only simplify the CI matrix.

## Keep supporting `onlyLastEvent`

Status: accepted

### Context

`onlyLastEvent` isn't part of the DCB specification, and a decision model that needs it for several query items can be built without it:

```php
$head = $eventStore->read(Query::all(), ReadOptions::create(limit: 1, backwards: true))->first()?->position;
// one read per query item, for example with backwards and limit: 1
$eventStore->append($events, AppendCondition::create(failIfEventsMatch: $mergedQuery, after: $head));
```

The head has to be determined first: taking `after` from the reads can lose an update, because an event matching the first read can land below the position returned by a later one. An event landing after the head that a read sees only makes the append fail, which is safe.

### Decision

`onlyLastEvent` keeps working in `read()` and in append conditions. The chained append above is recommended for new code.

| Topic | Decision |
|---|---|
| Cost of keeping it | About 15 lines and one test in this package. New read paths have to handle it |
| Owner | `QueryItem` of `wwwision/dcb-eventstore`. Rejecting it here only would make code written against `EventStore` fail depending on the adapter. Silently ignoring it isn't an option: reads would return every matching event |
| Removal | A later major version of the core package, after other adapters and `AtomicProjection` of `wwwision/dcb-tools` and `wwwision/dcb-library` have migrated |
| Benchmark before removal | A `limit: 1` read for several event types uses the derived table with all matching events, so it could be much slower than `MAX()`. This hasn't been measured |
| Derived table | Removing `onlyLastEvent` wouldn't remove it: using the `WHERE` clause for all queries was rejected because of query plans, see [the decision about `read()`](#apply-a-query-for-one-event-type-without-a-derived-table) |
| Append conditions | The flag makes no semantic difference there: the last match is after a position if any match is. Simplifying these queries is a separate change that requires benchmarks, because they run while the append lock is held |

### Consequences

The chained append takes one read more than the number of query items, instead of a single read. All of them have to read from the primary or an equally current source: a head from a replica that is further ahead than the one serving a read hides events below `after`, so the append can wrongly succeed. It also relies on [the identical sequence and visibility order](#serialize-appends-on-postgresql-to-keep-sequence-order-and-visibility-order-identical). A single query item doesn't need the head: read with `backwards` and `limit: 1`, and use the position of the returned event as `after`.

### Rejected alternatives

**Removing it in 7.0:** ties this release to a major version of the core package and to migrating every consumer, without a significant simplification here. Removing it later breaks the same code as removing it now.

## Keep extensions out of the core package

Status: accepted

### Context

`head()` and `createWaiter()` were added here and could be useful for other adapters.

### Decision

They stay in this package until a consumer outside of it needs them.

| Feature | Reason |
|---|---|
| `head()` | Adding it to `EventStore` breaks every implementation and requires a major version of the core package. `read(Query::all(), ReadOptions::create(limit: 1, backwards: true))` already determines the head with any adapter. It isn't part of the DCB specification either |
| `EventStoreWaiter` | No consumer outside of this package so far |

These guarantees aren't specific to this package though, and should be documented in `EventStore` of the core package: a reader never sees a position before all lower ones that are going to be committed, and failures of the underlying storage result in a `RuntimeException`, also while iterating over read events. A portable test for the order should get its own group in the test base of the core package, so that adapters can opt out while they are being checked. Until then, `AppendOrderingTest` stays here. None of this blocks a release of this package.

## Defer partitioned reads and notifications

Status: accepted

### Context

Partitioned reads (`readPartition()` with `TagPartition`) and notifications on PostgreSQL (`withNotifications()` with a `LISTEN`/`NOTIFY` waiter) were implemented for 7.0. Both are additive and can be released in a later minor version.

### Decision

Neither is part of 7.0. The implementation and its decision records are kept on the branch `feature/partitioned-reads-and-notify`.

| Feature | Reason |
|---|---|
| Partitioned reads | There is no evidence that reading is the bottleneck of slow projections. Reading 1000 events takes about 30ms, so handlers are the more likely cause, for example if they commit once per event. Several workers can already share a projection by filtering in PHP, see [Multiple workers for one projection](advanced.md#multiple-workers-for-one-projection). The hash would be a contract that can never change, partitions can overlap, SQLite isn't supported, and the subscription engine can't use it |
| Notifications | The only benefit is a latency below the poll interval, and no consumer needs that so far. A notification inside the append transaction makes every append on the server fail once the notification queue is full, for example because of a stuck worker |

### Revisit

- **Partitioned reads:** if measurements show that reading, not handling, is the bottleneck of a projection.
- **Notifications:** if a consumer needs a lower latency. Sending the notification best-effort after the commit, ignoring its failures, and listening only while waiting would avoid the failing appends.
