<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;

use function function_exists;
use function getenv;
use function is_string;
use function microtime;
use function pcntl_alarm;
use function pcntl_async_signals;
use function pcntl_signal;
use function proc_close;
use function proc_open;
use function usleep;

use const PHP_BINARY;
use const SIG_DFL;
use const SIGALRM;

#[CoversClass(DoctrineEventStore::class)]
final class EventStoreWaiterTest extends TestCase
{
    private const TABLE = 'dcb_waiter_test';

    private string $dsn;
    private Connection $connection;
    private Connection $waiterConnection;
    private DoctrineEventStore $eventStore;

    protected function setUp(): void
    {
        $dsn = getenv('DCB_TEST_DSN');
        // doctrine/dbal 3.6.0 fails to parse the equivalent "pdo-sqlite:///events_test.sqlite"
        $this->dsn = is_string($dsn) ? $dsn : 'pdo-sqlite://localhost/events_test.sqlite';
        $this->connection = $this->connect();
        $this->waiterConnection = $this->connect();
        $this->dropTables();
        $this->eventStore = new DoctrineEventStore(DoctrineEventStoreConfiguration::create($this->connection, self::TABLE));
        $this->eventStore->setup();
    }

    protected function tearDown(): void
    {
        $this->dropTables();
        $this->connection->close();
        $this->waiterConnection->close();
    }

    #[Test]
    public function first_call_returns_true_without_waiting(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);

        $start = microtime(true);
        self::assertTrue($waiter->waitForNewEvents(5.0));
        self::assertLessThan(2.0, microtime(true) - $start);
    }

    #[Test]
    public function returns_false_after_the_timeout_if_nothing_happened(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $start = microtime(true);
        self::assertFalse($waiter->waitForNewEvents(0.2));
        self::assertGreaterThanOrEqual(0.19, microtime(true) - $start);
    }

    #[Test]
    public function returns_true_once_for_events_appended_since_the_last_call(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));
        $this->eventStore->append(Event::create('SomeEvent', 'b', [], []));
        $this->eventStore->append(Event::create('SomeEvent', 'c', [], []));

        self::assertTrue($waiter->waitForNewEvents(5.0));
        self::assertFalse($waiter->waitForNewEvents(0.1));
    }

    #[Test]
    public function rejects_a_negative_timeout(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);

        $this->expectException(InvalidArgumentException::class);
        $waiter->waitForNewEvents(-0.1);
    }

    #[Test]
    public function rejects_a_connection_inside_a_transaction(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $this->waiterConnection->beginTransaction();

        try {
            $this->expectException(RuntimeException::class);
            $waiter->waitForNewEvents(0.0);
        } finally {
            $this->waiterConnection->rollBack();
        }
    }

    #[Test]
    public function detects_events_while_blocked(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection, 0.05);
        $waiter->waitForNewEvents(0.0);

        $process = $this->appendInSeparateProcess();
        $start = microtime(true);
        $result = $waiter->waitForNewEvents(10.0);
        $elapsed = microtime(true) - $start;
        proc_close($process);

        self::assertTrue($result);
        self::assertLessThan(5.0, $elapsed);
    }

    #[Test]
    public function a_signal_ends_the_wait_early(): void
    {
        if (!function_exists('pcntl_alarm')) {
            self::markTestSkipped('Requires the pcntl extension');
        }
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $previousAsyncSignals = pcntl_async_signals(true);
        pcntl_signal(SIGALRM, static function () {});
        pcntl_alarm(1);
        try {
            $start = microtime(true);
            $result = $waiter->waitForNewEvents(10.0);
            $elapsed = microtime(true) - $start;
        } finally {
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, SIG_DFL);
            pcntl_async_signals($previousAsyncSignals);
        }

        self::assertFalse($result);
        self::assertLessThan(5.0, $elapsed);
    }

    #[Test]
    public function reconnects_after_the_connection_was_closed(): void
    {
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $this->waiterConnection->close();

        self::assertFalse($waiter->waitForNewEvents(0.1));
        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));
        self::assertTrue($waiter->waitForNewEvents(5.0));
    }

    #[Test]
    public function throws_if_the_reconnect_fails_and_recovers_on_the_next_call(): void
    {
        $this->skipUnlessPostgreSql();
        $waiter = $this->eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);
        $this->connection->executeStatement('DROP TABLE ' . self::TABLE);
        $this->connection->fetchOne('SELECT pg_terminate_backend(?)', [$this->waiterConnection->fetchOne('SELECT pg_backend_pid()')]);
        usleep(100_000);

        try {
            $waiter->waitForNewEvents(0.0);
            self::fail('Expected the waiter to throw');
        } catch (RuntimeException) {
        }

        $this->eventStore->setup();
        self::assertTrue($waiter->waitForNewEvents(0.0));
    }

    private function skipUnlessPostgreSql(): void
    {
        if (!$this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            self::markTestSkipped('Only applies to PostgreSQL');
        }
    }

    /**
     * @return resource
     */
    private function appendInSeparateProcess()
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/append-event.php', $this->dsn, self::TABLE, 'data', 'some:tag', '0.3'], [], $pipes);
        self::assertIsResource($process);
        return $process;
    }

    private function dropTables(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
    }

    private function connect(): Connection
    {
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection((new DsnParser())->parse($this->dsn), $config);
    }
}
