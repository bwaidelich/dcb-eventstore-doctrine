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

### Upgrading PostgreSQL installations

Previous versions did not serialize appends on PostgreSQL:

- **Deployment:** stop all processes running a previous version before the new version starts appending. While both run, subscribers can miss events and append conditions can be violated.
- **Past events:** subscribers might have skipped events already, and this can't be detected or repaired automatically. If that is a concern, rebuild their projections.
- **Throughput:** appends no longer run in parallel, see [Event ordering](#event-ordering).

## Contribution

Contributions in the form of [issues](https://github.com/bwaidelich/dcb-eventstore-doctrine/issues) or [pull requests](https://github.com/bwaidelich/dcb-eventstore-doctrine/pulls) are highly appreciated

## License

See [LICENSE](./LICENSE)