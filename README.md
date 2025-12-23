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

## Contribution

Contributions in the form of [issues](https://github.com/bwaidelich/dcb-eventstore-doctrine/issues) or [pull requests](https://github.com/bwaidelich/dcb-eventstore-doctrine/pulls) are highly appreciated

## License

See [LICENSE](./LICENSE)