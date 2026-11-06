<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Closure;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\AbstractException;
use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Connection as DriverConnection;
use Doctrine\DBAL\Driver\Middleware;
use Doctrine\DBAL\Driver\Middleware\AbstractConnectionMiddleware;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Doctrine\DBAL\Driver\Middleware\AbstractResultMiddleware;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use JsonException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\TagPartition;

use function getenv;
use function is_string;
use function iterator_to_array;

/**
 * The event store must not expose exceptions of the database library, see docs/decisions.md
 */
#[CoversClass(DoctrineEventStore::class)]
final class DatabaseExceptionTest extends TestCase
{
    private const TABLE = 'dcb_database_exception_test';

    private string $dsn;
    private Connection $connection;
    private DoctrineEventStore $eventStore;

    protected function setUp(): void
    {
        $dsn = getenv('DCB_TEST_DSN');
        // doctrine/dbal 3.6.0 fails to parse the equivalent "pdo-sqlite:///events_test.sqlite"
        $this->dsn = is_string($dsn) ? $dsn : 'pdo-sqlite://localhost/events_test.sqlite';
        $this->connection = $this->connect();
        $this->dropTable();
        $this->eventStore = DoctrineEventStore::create($this->connection, self::TABLE);
        $this->eventStore->setup();
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        $connection = $this->connect();
        $connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $connection->close();
    }

    #[Test]
    public function read_wraps_database_exceptions(): void
    {
        $this->dropTable();

        self::assertWrapsDatabaseException(fn() => iterator_to_array($this->eventStore->read(Query::all())));
    }

    #[Test]
    public function read_partition_wraps_database_exceptions(): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
            self::markTestSkipped('Partitioned reads are not supported on SQLite');
        }
        $this->dropTable();

        self::assertWrapsDatabaseException(fn() => iterator_to_array($this->eventStore->readPartition(TagPartition::create('some:', 2, 0), Query::all())));
    }

    #[Test]
    public function head_wraps_database_exceptions(): void
    {
        $this->dropTable();

        self::assertWrapsDatabaseException(fn() => $this->eventStore->head());
    }

    #[Test]
    public function append_wraps_database_exceptions(): void
    {
        $this->dropTable();

        self::assertWrapsDatabaseException(fn() => $this->eventStore->append(Event::create('SomeEvent', 'a', [], [])));
    }

    #[Test]
    public function iterating_over_read_events_wraps_database_exceptions(): void
    {
        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));
        $failingConnection = $this->connect(new FailingFetchMiddleware());
        $eventStore = DoctrineEventStore::create($failingConnection, self::TABLE);

        try {
            $events = $eventStore->read(Query::all());
            self::assertWrapsDatabaseException(static fn() => iterator_to_array($events));
        } finally {
            $failingConnection->close();
        }
    }

    #[Test]
    public function append_keeps_the_wrapped_exception_if_the_rollback_fails(): void
    {
        if (!$this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            self::markTestSkipped('Only PostgreSQL rolls back within the append');
        }
        $failingConnection = $this->connect(new FailingRollbackMiddleware());
        $eventStore = DoctrineEventStore::create($failingConnection, self::TABLE);
        $this->dropTable();

        try {
            self::assertWrapsDatabaseException(static fn() => $eventStore->append(Event::create('SomeEvent', 'a', [], [])));
        } finally {
            $failingConnection->close();
        }
    }

    #[Test]
    public function append_wraps_json_exceptions_of_the_metadata(): void
    {
        self::assertWrapsException(JsonException::class, fn() => $this->eventStore->append(Event::create('SomeEvent', 'a', [], ['invalidUtf8' => "\xB1"])));
    }

    #[Test]
    public function iterating_over_read_events_wraps_json_exceptions_of_the_tags(): void
    {
        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));
        $corruptingConnection = $this->connect(new CorruptTagsMiddleware());
        $eventStore = DoctrineEventStore::create($corruptingConnection, self::TABLE);

        try {
            $events = $eventStore->read(Query::all());
            self::assertWrapsException(JsonException::class, static fn() => iterator_to_array($events));
        } finally {
            $corruptingConnection->close();
        }
    }

    /**
     * @param Closure(): mixed $callback
     */
    private static function assertWrapsDatabaseException(Closure $callback): void
    {
        self::assertWrapsException(DbalException::class, $callback);
    }

    /**
     * @param class-string<\Throwable> $expectedPrevious
     * @param Closure(): mixed $callback
     */
    private static function assertWrapsException(string $expectedPrevious, Closure $callback): void
    {
        try {
            $callback();
        } catch (\Throwable $e) {
            self::assertSame(RuntimeException::class, $e::class, 'Unexpected exception: ' . $e->getMessage());
            self::assertInstanceOf($expectedPrevious, $e->getPrevious());
            return;
        }
        self::fail('Expected an exception');
    }

    private function dropTable(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
    }

    private function connect(Middleware|null $middleware = null): Connection
    {
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        if ($middleware !== null) {
            $config->setMiddlewares([$middleware]);
        }
        return DriverManager::getConnection((new DsnParser())->parse($this->dsn), $config);
    }
}

/**
 * Fails when fetching rows, which can't be provoked otherwise because the drivers buffer the results of a query
 */
final class FailingFetchMiddleware implements Middleware
{
    public function wrap(DriverInterface $driver): DriverInterface
    {
        return new class ($driver) extends AbstractDriverMiddleware {
            public function connect(array $params): DriverConnection
            {
                return new class (parent::connect($params)) extends AbstractConnectionMiddleware {
                    public function query(string $sql): Result
                    {
                        return new class (parent::query($sql)) extends AbstractResultMiddleware {
                            public function fetchAssociative(): array|false
                            {
                                throw new class ('Failed to fetch') extends AbstractException {};
                            }
                        };
                    }
                };
            }
        };
    }
}

/**
 * Fails to roll back, like a connection that was lost while no statement was running
 */
final class FailingRollbackMiddleware implements Middleware
{
    public function wrap(DriverInterface $driver): DriverInterface
    {
        return new class ($driver) extends AbstractDriverMiddleware {
            public function connect(array $params): DriverConnection
            {
                return new class (parent::connect($params)) extends AbstractConnectionMiddleware {
                    public function exec(string $sql): int
                    {
                        if ($sql === 'ROLLBACK') {
                            throw new class ('Failed to roll back') extends AbstractException {};
                        }
                        return (int) parent::exec($sql);
                    }
                };
            }
        };
    }
}

/**
 * Returns rows with invalid tags, which the JSON columns of PostgreSQL and MySQL would not accept
 */
final class CorruptTagsMiddleware implements Middleware
{
    public function wrap(DriverInterface $driver): DriverInterface
    {
        return new class ($driver) extends AbstractDriverMiddleware {
            public function connect(array $params): DriverConnection
            {
                return new class (parent::connect($params)) extends AbstractConnectionMiddleware {
                    public function query(string $sql): Result
                    {
                        return new class (parent::query($sql)) extends AbstractResultMiddleware {
                            public function fetchAssociative(): array|false
                            {
                                $row = parent::fetchAssociative();
                                return $row === false ? false : ['tags' => 'not json'] + $row;
                            }
                        };
                    }
                };
            }
        };
    }
}
