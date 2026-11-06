# Advanced topics

The [README](../README.md) covers the basics. This document has the details for running the event store in production.

- [Event ordering](#event-ordering)
- [Multiple workers for one projection](#multiple-workers-for-one-projection)
- [Exceptions](#exceptions)
- [Waiting for new events](#waiting-for-new-events)

## Event ordering

A reader never sees an event before all events with a lower position that are going to be committed. Subscribers can safely checkpoint a position, and append conditions can rely on it. Positions can still have holes, for example after a rollback, so they must not be expected to be consecutive.

To guarantee this, appends to the same event table run one at a time. This limits the number of appends per second, depending on the latency of the database. See [decisions](decisions.md) for details and measurements. If that limit is a concern, append multiple events at once: the limit applies per append, not per event.

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

## Multiple workers for one projection

If a projection takes too long, first make sure that its handler commits once per batch instead of once per event. To share a projection between several workers, each worker can read the events and skip the entities it doesn't own:

```php
use Wwwision\DCBEventStore\Event\Tag;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\ReadOptions;

// worker 3 of 8 for all tags starting with "cart:"
$partitions = 8;
$partition = 3;
$owns = static fn(Tag $tag): bool => str_starts_with($tag->value, 'cart:') && crc32($tag->value) % $partitions === $partition;
$checkpoint = null; // load this from where the worker persists it

foreach ($eventStore->read(Query::all(), ReadOptions::create(from: $checkpoint?->next(), limit: 1000)) as $sequencedEvent) {
    foreach ($sequencedEvent->event->tags as $tag) {
        if ($owns($tag)) {
            handle($sequencedEvent, $tag); // handle the event for the entity identified by $tag
        }
    }
    $checkpoint = $sequencedEvent->position;
}
// persist $checkpoint
```

- **Cost:** every worker reads all events matching the query. Reading is usually much cheaper than handling, and a query for the relevant event types reduces it further.
- **Several matching tags:** an event is handled by every worker that owns one of its tags, so it is not suited for side effects that must happen once per event.
- **Changing the number of workers:** stop the affected workers first. Doubling the number of partitions `n` splits partition `k` into `k` and `k + n`, and both continue from the checkpoint of the former `k`. Any other change requires a rebuild, because most tags move.

## Exceptions

If the database fails or JSON can't be converted, the event store throws a `RuntimeException` with the original exception as `getPrevious()`. This also applies while iterating over read events, because they are fetched lazily.

The event store doesn't reconnect on its own. A long-running process can lose an idle connection, for example because of `wait_timeout` on MySQL/MariaDB, and the next call then fails. To recover, catch the `RuntimeException` and call `close()` on the connection: DBAL reconnects with the next statement. Before repeating an `append()`, keep in mind that the connection might have been lost after the commit. With an append condition that matches the appended events, the repeated call fails instead of appending them twice. Without one, the events can end up in the store twice.

## Waiting for new events

See [Background workers](../README.md#background-workers) for an example.

Read at least after every `true` that `waitForNewEvents()` returns. It can be spurious, and the first call returns it immediately, so that nothing gets lost between catching up and starting to wait. `false` means that the head didn't change since the last check, which can also happen before the timeout if a signal ended the wait.

The waiter calls `head()` once per interval (default: 1 second, see the second argument of `createWaiter()`), with a random deviation of up to 20%, so that workers don't query at the same time.

- **Dedicated connection:** the waiter needs a connection of its own that isn't inside a transaction.
- **Lost connections:** a waiter reconnects on its own and returns `true`. If that fails, it throws a `RuntimeException` and tries again on the next call, so a worker should catch it and back off.
