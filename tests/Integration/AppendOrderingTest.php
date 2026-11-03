<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

use function getenv;
use function is_resource;
use function is_string;
use function proc_close;
use function proc_open;
use function usleep;

use const PHP_BINARY;

/**
 * An append that is still in flight (connection A) must not allow a later one (process B) to become visible first.
 *
 * Connection A follows the protocol documented for writers that bypass the event store (see README).
 */
#[CoversClass(DoctrineEventStore::class)]
final class AppendOrderingTest extends TestCase
{
    private const TABLE = 'dcb_append_ordering_test';
    private const TAG = 'some:tag';
    /** Time that process B gets to overtake the in-flight append of connection A */
    private const OVERTAKE_WINDOW_MICROSECONDS = 300_000;

    private string $dsn;
    private Connection $connectionA;
    private Connection $readerConnection;
    private DoctrineEventStore $eventStore;

    protected function setUp(): void
    {
        $dsn = getenv('DCB_TEST_DSN');
        $this->dsn = is_string($dsn) ? $dsn : 'pdo-sqlite://localhost/events_test.sqlite';
        $this->connectionA = $this->connect();
        $this->readerConnection = $this->connect();
        $this->readerConnection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->eventStore = DoctrineEventStore::create($this->readerConnection, self::TABLE);
        $this->eventStore->setup();
    }

    protected function tearDown(): void
    {
        $this->readerConnection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->readerConnection->close();
        $this->connectionA->close();
    }

    #[Test]
    public function subscriber_does_not_miss_an_event_that_was_in_flight_when_it_checkpointed(): void
    {
        $this->beginAppendOnConnectionA();
        $processB = $this->startAppendInSeparateProcess();
        usleep(self::OVERTAKE_WINDOW_MICROSECONDS);

        $checkpoint = 0;
        $received = [];
        foreach ($this->eventStore->read(Query::all()) as $sequencedEvent) {
            $received[] = $sequencedEvent->event->data->value;
            $checkpoint = $sequencedEvent->position->value;
        }

        $this->commitAppendOnConnectionA();
        self::assertSame(0, proc_close($processB), 'Append in separate process failed');

        foreach ($this->eventStore->read(Query::all(), ReadOptions::create(from: $checkpoint + 1)) as $sequencedEvent) {
            $received[] = $sequencedEvent->event->data->value;
        }
        self::assertEqualsCanonicalizing(['A', 'B'], $received);
    }

    #[Test]
    public function conditional_append_fails_if_an_event_was_in_flight_during_the_decision(): void
    {
        $this->beginAppendOnConnectionA();
        $processB = $this->startAppendInSeparateProcess();
        usleep(self::OVERTAKE_WINDOW_MICROSECONDS);

        $query = Query::fromItems(QueryItem::create(tags: [self::TAG]));
        $after = $this->eventStore->read($query, ReadOptions::create(backwards: true))->first()?->position;

        $this->commitAppendOnConnectionA();
        self::assertSame(0, proc_close($processB), 'Append in separate process failed');

        $this->expectException(ConditionalAppendFailed::class);
        $this->eventStore->append(Event::create('SomeEvent', 'C', [self::TAG], []), AppendCondition::create($query, $after));
    }

    // -------------------------------------

    private function connect(): Connection
    {
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection((new DsnParser())->parse($this->dsn), $config);
    }

    private function beginAppendOnConnectionA(): void
    {
        $platform = $this->connectionA->getDatabasePlatform();
        $lockName = 'dcb_' . self::TABLE;
        if ($platform instanceof PostgreSQLPlatform) {
            $this->connectionA->executeStatement('BEGIN');
            $this->connectionA->fetchOne('SELECT pg_advisory_xact_lock(hashtextextended(?, 0))', [$lockName]);
        } elseif ($platform instanceof AbstractMySQLPlatform) {
            $this->connectionA->fetchOne('SELECT GET_LOCK(?, 30)', [$lockName]);
            $this->connectionA->executeStatement('BEGIN');
        } elseif ($platform instanceof SQLitePlatform) {
            $this->connectionA->executeStatement('BEGIN IMMEDIATE');
        } else {
            self::fail('Unsupported platform ' . $platform::class);
        }
        $this->connectionA->insert(self::TABLE, [
            'type' => 'SomeEvent',
            'data' => 'A',
            'metadata' => '{}',
            'tags' => '["' . self::TAG . '"]',
            'recorded_at' => '2026-01-01 00:00:00',
        ]);
    }

    private function commitAppendOnConnectionA(): void
    {
        $this->connectionA->executeStatement('COMMIT');
        if ($this->connectionA->getDatabasePlatform() instanceof AbstractMySQLPlatform) {
            $this->connectionA->fetchOne('SELECT RELEASE_LOCK(?)', ['dcb_' . self::TABLE]);
        }
    }

    /**
     * @return resource
     */
    private function startAppendInSeparateProcess()
    {
        $process = proc_open([PHP_BINARY, __DIR__ . '/append-event.php', $this->dsn, self::TABLE, 'B', self::TAG], [], $pipes);
        if (!is_resource($process)) {
            throw new RuntimeException('Failed to start process');
        }
        return $process;
    }
}
