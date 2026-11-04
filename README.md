# Dynamic Consistency Boundary Event Store - Doctrine DBAL adapter

[Doctrine DBAL](https://www.doctrine-project.org/projects/doctrine-dbal/en/current/index.html) adapter for the [Dynamic Consistency Boundary implementation](https://github.com/bwaidelich/dcb-eventstore).

## Usage

Install via [composer](https://getcomposer.org):

```shell
composer require wwwision/dcb-eventstore-doctrine
```

### Create instance

```php
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

$dsn = 'pdo-mysql://user:password@127.0.0.1:3306/database';
$connection = DriverManager::getConnection((new DsnParser())->parse($dsn));
$eventStore = DoctrineEventStore::create($connection, eventTableName: 'events');
```

### Automatically create required database tables

```php
// ...
$eventStore->setup();
```

See [wwwision/dcb-eventstore](https://github.com/bwaidelich/dcb-eventstore) for more details and usage examples

### Event ordering

A reader never sees an event before all events with a lower position that are going to be committed. Subscribers can safely checkpoint a position, and append conditions can rely on it. Positions can still have holes, for example after a rollback, so they must not be expected to be consecutive.

To guarantee this, appends to the same event table run one at a time. This limits the number of appends per second, depending on the latency of the database. See [docs/decisions.md](docs/decisions.md) for details and measurements. If that limit is a concern, append multiple events at once: the limit applies per append, not per event.

The guarantee only covers appends through this package on a single primary database, so it doesn't hold for multi-primary setups like Galera.

Anything else that writes to the event table has to take the same lock until after the commit:

```sql
-- PostgreSQL
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('dcb_<table>', 0));
INSERT INTO <table> ...;
COMMIT;

-- MySQL/MariaDB
SELECT GET_LOCK('dcb_<table>', 30);
INSERT INTO <table> ...;
SELECT RELEASE_LOCK('dcb_<table>');
```

### Partitioned reads

Multiple workers can share one projection, without each of them reading the full event log. `readPartition()` only returns events with a tag that starts with a given prefix and hashes into one of `n` partitions. This is an extension of this package and not part of the DCB specification. It is supported on PostgreSQL and MySQL/MariaDB, and throws an exception on SQLite.

```php
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStoreDoctrine\TagPartition;

// worker 3 of 8 for all tags starting with "cart:"
$partition = TagPartition::create(prefix: 'cart:', partitions: 8, partition: 3);
$batchSize = 1000;

$head = $eventStore->head();
if ($head === null) {
    return; // the event store is empty
}
$numberOfEvents = 0;
foreach ($eventStore->readPartition($partition, Query::all(), ReadOptions::create(from: $checkpoint->next(), limit: $batchSize), until: $head) as $sequencedEvent) {
    foreach ($sequencedEvent->event->tags as $tag) {
        if ($partition->contains($tag)) {
            // handle the event for the entity identified by $tag
        }
    }
    $checkpoint = $sequencedEvent->position;
    $numberOfEvents++;
}
if ($numberOfEvents < $batchSize) {
    // all positions up to $head were checked. Without this, a worker would rescan positions without matching events on every poll
    $checkpoint = $head;
}
// persist $checkpoint
```

- **Several matching tags:** an event is returned by every partition that owns one of its tags, so partitions can overlap. Use `TagPartition::contains()` as above to only handle the tags of the current partition. Because of that overlap, partitions are not suited for side effects that must happen once per event.
- **No matching tag:** the event isn't returned by any partition.
- **Prefix:** literal and case-sensitive. A trailing colon is not enforced, so `cart` also matches `cartoon:1`.
- **Query:** only events matching both the partition and the query are returned. Query items with `onlyLastEvent` are rejected.
- **`until`:** inclusive, and can't be combined with reading backwards. Use the global `head()`, not the position of the last event of a partition.
- **Tags:** removing a tag from new events is a breaking change for partitioned readers, because they stop receiving those events.
- **Performance:** there is no index for partitions, so reads walk the primary key from `from` up to `until` or until `limit` is reached. See [docs/decisions.md](docs/decisions.md).

The partition of a tag is fixed: the first 32 bits of the SHA-256 of the whole tag as unsigned integer, modulo the number of partitions.

#### Changing the number of partitions

Checkpoints belong to the caller, so this package can't enforce the following. A previous and a new worker must never process the same tags at the same time, so stop the affected workers first.

- **Doubling `n`:** partition `k` splits into `k` and `k + n`. Both start from the checkpoint of the former `k`. This works for any `n`, not only for powers of two. State that is kept per worker has to move along with its tags.
- **Halving `n`:** partitions `k` and `k + n` merge into `k`, which continues from the lower of both checkpoints. It sees some events again, so handlers must be idempotent.
- **Any other change:** rebuild the projection, because most tags move to a different partition.

### Exceptions

If the database fails or JSON can't be converted, the event store throws a `RuntimeException` with the original exception as `getPrevious()`. This also applies while iterating over read events, because they are fetched lazily.

The event store doesn't reconnect on its own. A long-running process can lose an idle connection, for example because of `wait_timeout` on MySQL/MariaDB, and the next call then fails. To recover, catch the `RuntimeException` and call `close()` on the connection: DBAL reconnects with the next statement. Before repeating an `append()`, keep in mind that the connection might have been lost after the commit. With an append condition that matches the appended events, the repeated call fails instead of appending them twice. Without one, the events can end up in the store twice.

### Waiting for new events

An idle worker can block until there might be new events instead of polling on its own:

```php
$config = DoctrineEventStoreConfiguration::create($connection, 'events')->withNotifications();
$eventStore = new DoctrineEventStore($config);

$waiter = $eventStore->createWaiter($dedicatedConnection);
while (true) {
    // read and handle new events after the last checkpoint...
    $waiter->waitForNewEvents(30.0);
}
```

Read at least after every `true`. It can be spurious, and the first call returns it immediately, so that nothing gets lost between catching up and starting to wait. `false` means that the head didn't change since the last check, which can also happen before the timeout if a signal ended the wait.

On PostgreSQL, `withNotifications()` makes appends send a notification on commit, and the waiter wakes up right away. Otherwise, the waiter calls `head()` once per interval (default: 1 second, see the second argument of `createWaiter()`). This also applies to MySQL/MariaDB and SQLite, where `withNotifications()` has no effect.

- **Writers and workers:** enable `withNotifications()` for both. If only the worker has it, it notices events when its timeout expires. If only the writers have it, the worker polls.
- **Cost:** notifications slow down appends, even across event tables, because PostgreSQL serializes notifying commits. See [docs/decisions.md](docs/decisions.md) for measurements.
- **Dedicated connection:** on PostgreSQL it can't be used for anything else while a worker is running, must use the `pdo_pgsql` driver and has to connect to the primary. `LISTEN` doesn't work through PgBouncer in transaction mode, so it needs to bypass that.
- **Other writers:** writers outside of this package don't notify. Their events are noticed when the timeout expires.
- **Lost connections:** a waiter reconnects on its own and returns `true`. If that fails, it throws a `RuntimeException` and tries again on the next call, so a worker should catch it and back off. A silently dropped TCP connection can block until the operating system gives up. A timeout of 30 seconds or less keeps the connection from looking idle to firewalls.
- **Load:** under constant appends workers read constantly, which can cause more load than polling. A worker can sleep after catching up to limit this.
- **Stuck workers:** a listener only drains while its worker waits. If a worker hangs or is busy for long with its connection open, the notification queue of the PostgreSQL server fills up (8 GB by default). Once it is full, appends with notifications fail with a `RuntimeException`, also for other event tables. Monitor `pg_notification_queue_usage()`, and call `waitForNewEvents(0.0)` periodically during a long catch-up. Don't ignore a `true` from such a call: read again afterwards, otherwise the worker can wait although there are events it hasn't read.

### Upgrading PostgreSQL installations

Previous versions did not serialize appends on PostgreSQL:

- **Deployment:** stop all processes running a previous version before the new version starts appending. While both run, subscribers can miss events and append conditions can be violated.
- **Past events:** subscribers might have skipped events already, and this can't be detected or repaired automatically. If that is a concern, rebuild their projections.
- **Throughput:** appends no longer run in parallel, see [Event ordering](#event-ordering).

## Contribution

Contributions in the form of [issues](https://github.com/bwaidelich/dcb-eventstore-doctrine/issues) or [pull requests](https://github.com/bwaidelich/dcb-eventstore-doctrine/pulls) are highly appreciated

## License

See [LICENSE](./LICENSE)