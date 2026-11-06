# DCB Event Store for Doctrine DBAL

Store events in **PostgreSQL, MySQL/MariaDB or SQLite** and enforce business rules across them, without having to design aggregates up front.

This package is the [Doctrine DBAL](https://www.doctrine-project.org/projects/doctrine-dbal/en/current/index.html) adapter for [wwwision/dcb-eventstore](https://github.com/bwaidelich/dcb-eventstore), a PHP implementation of [Dynamic Consistency Boundaries](https://dcb.events/) (DCB).

**Contents:** [Quick start](#quick-start) · [Core concepts](#core-concepts) · [Examples](#examples) · [Using a real database](#using-a-real-database) · [Background workers](#background-workers) · [Good to know](#good-to-know) · [Further reading](#further-reading)

## Quick start

Requires PHP 8.3 or later and the PDO driver of your database.

```shell
composer require wwwision/dcb-eventstore-doctrine
```

The following script is complete and runs as is, using an in-memory SQLite database:

```php
use Doctrine\DBAL\DriverManager;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

require 'vendor/autoload.php';

$connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
$eventStore = DoctrineEventStore::create($connection, eventTableName: 'events');

// Creates the event table. Safe to run repeatedly
$eventStore->setup();

$eventStore->append(Event::create(
    type: 'CourseDefined',
    data: ['id' => 'c1', 'capacity' => 2],
    tags: 'course:c1',
));

foreach ($eventStore->read(Query::all()) as $sequencedEvent) {
    echo "#{$sequencedEvent->position->value} {$sequencedEvent->event->type->value} {$sequencedEvent->event->data}\n";
}
```

```text
#1 CourseDefined {"id":"c1","capacity":2}
```

## Core concepts

| Term | Meaning |
|---|---|
| **Event** | Something that happened. Has a type, data, optional metadata and any number of tags |
| **Tag** | Marks what an event is about, for example `course:c1` or `student:alice`. One event can have several |
| **Position** | Where an event is in the global, ordered log. Positions increase, but can have holes |
| **Query** | Selects events by type and/or tag. This is how you read, and how you describe which events affect a decision |
| **Append condition** | "Only store these events if no event matches this query (after this position)". This is what keeps your data consistent |

A write usually has three steps: read the events that are relevant for a decision, make the decision, then append the result on the condition that no relevant event was added in the meantime.

## Examples

The examples build on each other and on the `$eventStore` of the quick start. They are part of the test suite, including their output.

### Do something only once

An append condition without a position fails if any matching event exists. Use it for anything that has to be unique, like an ID or an email address:

```php
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;

$event = Event::create('CourseDefined', ['id' => 'c2', 'capacity' => 1], 'course:c2');
$courseMustNotExist = AppendCondition::create(
    Query::fromItems(QueryItem::create(eventTypes: 'CourseDefined', tags: 'course:c2')),
);

$eventStore->append($event, $courseMustNotExist);

try {
    $eventStore->append($event, $courseMustNotExist);
} catch (ConditionalAppendFailed) {
    echo "Course c2 exists already\n";
}
```

```text
Course c2 exists already
```

### Enforce a business rule

A course must not have more subscriptions than its capacity. No aggregate is needed: the query defines exactly which events the decision depends on.

```php
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;

function subscribe(EventStore $eventStore, string $courseId, string $studentId): void
{
    // 1. All events that affect this decision
    $query = Query::fromItems(QueryItem::create(
        eventTypes: ['CourseDefined', 'StudentSubscribed'],
        tags: "course:$courseId",
    ));

    // 2. Build the state to decide on
    $capacity = 0;
    $subscriptions = 0;
    $lastPosition = null;
    foreach ($eventStore->read($query) as $sequencedEvent) {
        $lastPosition = $sequencedEvent->position;
        if ($sequencedEvent->event->type->value === 'CourseDefined') {
            $capacity = $sequencedEvent->event->data->jsonDecode()['capacity'];
        } else {
            $subscriptions++;
        }
    }
    if ($subscriptions >= $capacity) {
        throw new DomainException("Course $courseId is full");
    }

    // 3. Append, unless a relevant event was added since step 1
    $eventStore->append(
        Event::create(
            type: 'StudentSubscribed',
            data: ['courseId' => $courseId, 'studentId' => $studentId],
            tags: ["course:$courseId", "student:$studentId"],
        ),
        AppendCondition::create($query, after: $lastPosition),
    );
}

subscribe($eventStore, 'c1', 'alice');
subscribe($eventStore, 'c1', 'bob');

try {
    subscribe($eventStore, 'c1', 'carol');
} catch (DomainException $e) {
    echo $e->getMessage() . "\n";
}
```

```text
Course c1 is full
```

### Handle concurrent writes

If two requests decide on the same state, only the first one can append. The other one gets a `ConditionalAppendFailed`, because its decision is based on outdated state:

```php
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;

$query = Query::fromItems(QueryItem::create(eventTypes: 'StudentSubscribed', tags: 'course:c2'));

// Both requests read the same state...
$position = $eventStore->head();

// ...the first one appends...
$eventStore->append(
    Event::create('StudentSubscribed', ['courseId' => 'c2', 'studentId' => 'dave'], ['course:c2', 'student:dave']),
    AppendCondition::create($query, after: $position),
);

// ...and the second one is rejected
try {
    $eventStore->append(
        Event::create('StudentSubscribed', ['courseId' => 'c2', 'studentId' => 'erin'], ['course:c2', 'student:erin']),
        AppendCondition::create($query, after: $position),
    );
} catch (ConditionalAppendFailed) {
    echo "Conflict\n";
}
```

```text
Conflict
```

To recover, repeat the whole decision, because the outcome might be a different one now. Here the retry finds out that the course is full:

```php
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;

for ($attempt = 1; $attempt <= 3; $attempt++) {
    try {
        subscribe($eventStore, 'c2', 'erin');
        break;
    } catch (ConditionalAppendFailed) {
        // the state has changed, decide again
    } catch (DomainException $e) {
        echo $e->getMessage() . "\n";
        break;
    }
}
```

```text
Course c2 is full
```

### Read events

Tags within a `QueryItem` have to match all (AND), event types any (OR). Multiple items select the events that match any of the items (OR).

```php
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\ReadOptions;

// Everything about one student
foreach ($eventStore->read(Query::fromItems(QueryItem::create(tags: 'student:alice'))) as $sequencedEvent) {
    echo "Alice: {$sequencedEvent->event->type->value}\n";
}

// Several independent criteria
$query = Query::fromItems(
    QueryItem::create(eventTypes: 'CourseDefined', tags: 'course:c2'),
    QueryItem::create(eventTypes: 'StudentSubscribed', tags: ['course:c1', 'student:bob']),
);
foreach ($eventStore->read($query) as $sequencedEvent) {
    echo "Any: {$sequencedEvent->event->data}\n";
}

// The latest event of a type, or NULL if there is none
$latest = $eventStore->read(
    Query::fromItems(QueryItem::create(eventTypes: 'CourseDefined')),
    ReadOptions::create(limit: 1, backwards: true),
)->first();
echo "Latest: {$latest?->event->data}\n";

// A batch of events, starting from a position
foreach ($eventStore->read(Query::all(), ReadOptions::create(from: 2, limit: 2)) as $sequencedEvent) {
    echo "Batch: #{$sequencedEvent->position->value}\n";
}
```

```text
Alice: StudentSubscribed
Any: {"id":"c2","capacity":1}
Any: {"courseId":"c1","studentId":"bob"}
Latest: {"id":"c2","capacity":1}
Batch: #2
Batch: #3
```

### Append several events at once

Multiple events are stored atomically: all of them or none. This is also the way to go for imports, see [Good to know](#good-to-know). Metadata is optional and useful for anything that isn't part of the event itself:

```php
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;

$eventStore->append(Events::fromArray([
    Event::create('CourseDefined', ['id' => 'c3', 'capacity' => 10], 'course:c3', metadata: ['importedBy' => 'admin']),
    Event::create('CourseDefined', ['id' => 'c4', 'capacity' => 20], 'course:c4', metadata: ['importedBy' => 'admin']),
]));
```

## Using a real database

Replace the connection of the quick start with one for your database, and run `setup()` when deploying:

```php
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

$dsn = 'pdo-pgsql://user:password@127.0.0.1:5432/database'; // or: pdo-mysql://user:password@127.0.0.1:3306/database

$connection = DriverManager::getConnection((new DsnParser())->parse($dsn));
$eventStore = DoctrineEventStore::create($connection, eventTableName: 'events');
$eventStore->setup();
```

| | PostgreSQL | MySQL | MariaDB | SQLite |
|---|---|---|---|---|
| Tested versions | 16, 17 | 8.0, 8.4 | 10.11, 11.4 | |
| `append()`, `read()`, `head()` | ✓ | ✓ | ✓ | ✓ |
| Waiting for new events | Instant or polling | Polling | Polling | Polling |
| [Partitioned reads](docs/advanced.md#partitioned-reads) | ✓ | ✓ | ✓ | |

Doctrine DBAL 3.6+ and 4 are supported.

## Background workers

A worker, for example one that updates a read model, processes the event log from its last position and then waits for new events:

```php
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Tools\DsnParser;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;

$config = DoctrineEventStoreConfiguration::create($connection, 'events')->withNotifications();
$eventStore = new DoctrineEventStore($config);

// The waiter needs a connection of its own
$waiterConnection = DriverManager::getConnection((new DsnParser())->parse($dsn));
$waiter = $eventStore->createWaiter($waiterConnection);

$nextPosition = null; // Load this from where the worker persists it
while (true) {
    foreach ($eventStore->read(Query::all(), ReadOptions::create(from: $nextPosition)) as $sequencedEvent) {
        handle($sequencedEvent); // Your code
        $nextPosition = $sequencedEvent->position->next(); // Persist this along with the result of handle()
    }
    $waiter->waitForNewEvents(30.0);
}
```

On PostgreSQL, the worker wakes up right away if `withNotifications()` is enabled for writers and workers, which slows down appends. Everywhere else, the waiter polls once per second. Before using this in production, see [Waiting for new events](docs/advanced.md#waiting-for-new-events).

To distribute a projection across several workers, see [Partitioned reads](docs/advanced.md#partitioned-reads).

## Good to know

- **No surrounding transaction:** `append()` fails if a transaction is active on its connection, so events can't be appended as part of a transaction of your application. Use a separate connection if needed.
- **Throughput:** appends to the same event table run one at a time, which limits the number of appends per second. The limit applies per append and not per event, so append events together if you can. [Details](docs/advanced.md#event-ordering)
- **Reliable order:** a reader never sees an event before all events with a lower position that are going to be committed, so it is safe to persist a position. This doesn't hold for multi-primary setups like Galera, or for writers that bypass this package. [Details](docs/advanced.md#event-ordering)
- **Errors:** database and JSON failures result in a `RuntimeException`, also while iterating over read events. `ConditionalAppendFailed` extends `RuntimeException`, so catch it first. [Details](docs/advanced.md#exceptions)
- **Lost connections:** the event store doesn't reconnect on its own, which matters for long-running processes. [Details](docs/advanced.md#exceptions)

## Further reading

- [Advanced topics](docs/advanced.md): event ordering, partitioned reads, exceptions, waiting for new events
- [Upgrading to 7.0](docs/upgrading.md)
- [Decisions](docs/decisions.md): why things are the way they are, including measurements
- [wwwision/dcb-eventstore](https://github.com/bwaidelich/dcb-eventstore): the core package with its full API
- [dcb.events](https://dcb.events/): introduction to Dynamic Consistency Boundaries and its specification

## Contribution

Contributions in the form of [issues](https://github.com/bwaidelich/dcb-eventstore-doctrine/issues) or [pull requests](https://github.com/bwaidelich/dcb-eventstore-doctrine/pulls) are highly appreciated.

`composer test` runs all checks using SQLite. Set the environment variable `DCB_TEST_DSN` to run them against another database, for example `pdo-pgsql://user:password@127.0.0.1:5432/database`.

## License

See [LICENSE](./LICENSE)
