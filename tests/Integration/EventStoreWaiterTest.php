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
use PDO;
use Pdo\Pgsql;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;
use Wwwision\DCBEventStoreDoctrine\EventStoreWaiter;
use Wwwision\DCBEventStoreDoctrine\PollingWaiter;
use Wwwision\DCBEventStoreDoctrine\PostgresNotificationWaiter;

use function extension_loaded;
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
    private const OTHER_TABLE = 'dcb_waiter_test_other';

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

        self::assertInstanceOf(EventStoreWaiter::class, $waiter);
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
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection, 0.05);
        $waiter->waitForNewEvents(0.0);

        $process = $this->appendInSeparateProcess(self::TABLE, delaySeconds: 0.3);
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
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection);
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
    public function a_successful_append_notifies_listeners_on_postgresql(): void
    {
        $this->skipUnlessPostgreSql();
        $this->listenOnWaiterConnection();

        $this->notifyingEventStore()->append(Event::create('SomeEvent', 'a', [], []));

        self::assertSame(1, $this->countNotifications());
    }

    #[Test]
    public function a_failed_append_condition_does_not_notify_listeners(): void
    {
        $this->skipUnlessPostgreSql();
        $eventStore = $this->notifyingEventStore();
        $eventStore->append(Event::create('SomeEvent', 'a', ['some:tag'], []));
        $this->listenOnWaiterConnection();

        try {
            $eventStore->append(Event::create('SomeEvent', 'b', ['some:tag'], []), AppendCondition::create(Query::fromItems(QueryItem::create(tags: ['some:tag']))));
            self::fail('Expected the append to fail');
        } catch (ConditionalAppendFailed) {
        }

        self::assertSame(0, $this->countNotifications());
    }

    #[Test]
    public function appends_do_not_notify_listeners_by_default(): void
    {
        $this->skipUnlessPostgreSql();
        $this->listenOnWaiterConnection();

        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));

        self::assertSame(0, $this->countNotifications());
    }

    #[Test]
    public function appended_events_are_visible_after_a_notifying_append(): void
    {
        $this->skipUnlessPostgreSql();
        $eventStore = $this->notifyingEventStore();

        $eventStore->append(Event::create('SomeEvent', 'a', [], []));

        $otherConnection = $this->connect();
        self::assertEquals(1, $otherConnection->fetchOne('SELECT COUNT(*) FROM ' . self::TABLE));
        $otherConnection->close();
    }

    #[Test]
    public function uses_notifications_only_on_postgresql_with_notifications_enabled(): void
    {
        $isPostgreSql = $this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform;

        self::assertInstanceOf($isPostgreSql ? PostgresNotificationWaiter::class : PollingWaiter::class, $this->notifyingEventStore()->createWaiter($this->waiterConnection));
        self::assertInstanceOf(PollingWaiter::class, $this->eventStore->createWaiter($this->waiterConnection));
    }

    #[Test]
    public function wakes_up_immediately_when_notified_while_blocked(): void
    {
        $this->skipUnlessPostgreSql();
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection, 60.0);
        self::assertInstanceOf(PostgresNotificationWaiter::class, $waiter);
        $waiter->waitForNewEvents(0.0);

        $process = $this->appendInSeparateProcess(self::TABLE, delaySeconds: 0.3);
        $start = microtime(true);
        $result = $waiter->waitForNewEvents(10.0);
        $elapsed = microtime(true) - $start;
        proc_close($process);

        self::assertTrue($result);
        self::assertGreaterThan(0.1, $elapsed);
        self::assertLessThan(5.0, $elapsed);
    }

    #[Test]
    public function coalesces_queued_notifications(): void
    {
        $this->skipUnlessPostgreSql();
        $eventStore = $this->notifyingEventStore();
        $waiter = $eventStore->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $eventStore->append(Event::create('SomeEvent', 'a', [], []));
        $eventStore->append(Event::create('SomeEvent', 'b', [], []));
        $eventStore->append(Event::create('SomeEvent', 'c', [], []));
        // Notifications are delivered asynchronously. One that arrives after the others were coalesced causes a spurious wake-up, which is allowed
        usleep(300_000);

        self::assertTrue($waiter->waitForNewEvents(5.0));
        self::assertFalse($waiter->waitForNewEvents(0.1));
    }

    #[Test]
    public function detects_events_of_writers_without_notifications_when_the_timeout_expires(): void
    {
        $this->skipUnlessPostgreSql();
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $this->eventStore->append(Event::create('SomeEvent', 'a', [], []));

        $start = microtime(true);
        self::assertTrue($waiter->waitForNewEvents(0.2));
        self::assertGreaterThanOrEqual(0.19, microtime(true) - $start);
    }

    #[Test]
    public function is_not_woken_up_by_other_event_tables(): void
    {
        $this->skipUnlessPostgreSql();
        $otherEventStore = new DoctrineEventStore(DoctrineEventStoreConfiguration::create($this->connection, self::OTHER_TABLE)->withNotifications());
        $otherEventStore->setup();
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection);
        $waiter->waitForNewEvents(0.0);

        $otherEventStore->append(Event::create('SomeEvent', 'a', [], []));

        self::assertFalse($waiter->waitForNewEvents(0.2));
    }

    #[Test]
    public function reconnects_and_keeps_listening_after_losing_the_connection(): void
    {
        $this->skipUnlessPostgreSql();
        $eventStore = $this->notifyingEventStore();
        $waiter = $eventStore->createWaiter($this->waiterConnection, 60.0);
        $waiter->waitForNewEvents(0.0);
        $this->connection->fetchOne('SELECT pg_terminate_backend(?)', [$this->waiterConnection->fetchOne('SELECT pg_backend_pid()')]);
        usleep(100_000);

        self::assertTrue($waiter->waitForNewEvents(5.0));
        self::assertFalse($waiter->waitForNewEvents(0.1));
        $eventStore->append(Event::create('SomeEvent', 'a', [], []));

        $start = microtime(true);
        self::assertTrue($waiter->waitForNewEvents(10.0));
        self::assertLessThan(5.0, microtime(true) - $start);
    }

    #[Test]
    public function throws_if_the_reconnect_fails_and_recovers_on_the_next_call(): void
    {
        $this->skipUnlessPostgreSql();
        $waiter = $this->notifyingEventStore()->createWaiter($this->waiterConnection);
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

    #[Test]
    public function rejects_a_connection_without_pdo(): void
    {
        $this->skipUnlessPostgreSql();
        if (!extension_loaded('pgsql')) {
            self::markTestSkipped('Requires the pgsql extension');
        }
        $connection = DriverManager::getConnection(['driver' => 'pgsql'] + $this->connection->getParams());
        $waiter = $this->notifyingEventStore()->createWaiter($connection);

        try {
            $this->expectException(RuntimeException::class);
            $this->expectExceptionMessageMatches('/pdo_pgsql/');
            $waiter->waitForNewEvents(0.0);
        } finally {
            $connection->close();
        }
    }

    private function skipUnlessPostgreSql(): void
    {
        if (!$this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            self::markTestSkipped('Only applies to PostgreSQL');
        }
    }

    private function listenOnWaiterConnection(): void
    {
        $this->waiterConnection->executeStatement('LISTEN ' . DoctrineEventStoreConfiguration::create($this->waiterConnection, self::TABLE)->notificationChannel());
    }

    private function countNotifications(): int
    {
        $pdo = $this->waiterConnection->getNativeConnection();
        self::assertInstanceOf(PDO::class, $pdo);
        $method = $pdo instanceof Pgsql ? 'getNotify' : 'pgsqlGetNotify';
        $count = 0;
        while ($pdo->{$method}(PDO::FETCH_ASSOC, 200) !== false) {
            $count++;
        }
        return $count;
    }

    /**
     * @return resource
     */
    private function appendInSeparateProcess(string $table, float $delaySeconds)
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/append-event.php', $this->dsn, $table, 'data', 'some:tag', 'notify', (string) $delaySeconds], [], $pipes);
        self::assertIsResource($process);
        return $process;
    }

    private function notifyingEventStore(): DoctrineEventStore
    {
        return new DoctrineEventStore(DoctrineEventStoreConfiguration::create($this->connection, self::TABLE)->withNotifications());
    }

    private function dropTables(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::OTHER_TABLE);
    }

    private function connect(): Connection
    {
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection((new DsnParser())->parse($this->dsn), $config);
    }
}
