<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStore\SequencedEvent\SequencePosition;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\TagPartition;

use function array_sum;
use function array_unique;
use function array_values;
use function count;
use function getenv;
use function in_array;
use function is_string;
use function max;
use function min;
use function range;
use function sort;
use function sprintf;

#[CoversClass(DoctrineEventStore::class)]
#[CoversClass(TagPartition::class)]
final class PartitionedReadTest extends TestCase
{
    private const TABLE = 'dcb_partition_test';

    private Connection $connection;
    private DoctrineEventStore $eventStore;

    protected function setUp(): void
    {
        $this->connection = self::createConnection();
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->eventStore = DoctrineEventStore::create($this->connection, self::TABLE);
        $this->eventStore->setup();
    }

    protected function tearDown(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->connection->close();
    }

    #[Test]
    public function head_returns_null_for_an_empty_event_store(): void
    {
        self::assertNull($this->eventStore->head());
    }

    #[Test]
    public function head_returns_the_position_of_the_last_event(): void
    {
        $this->eventStore->append(Event::create('SomeEvent', 'a'));
        $this->eventStore->append(Event::create('SomeEvent', 'b'));

        self::assertSame(2, $this->eventStore->head()?->value);
    }

    #[Test]
    public function head_ignores_failed_appends(): void
    {
        $this->eventStore->append(Event::create('SomeEvent', 'a'));
        try {
            $this->eventStore->append(Event::create('SomeEvent', 'b'), AppendCondition::create(Query::fromItems(QueryItem::create(eventTypes: 'SomeEvent'))));
            self::fail('The append should have failed');
        } catch (ConditionalAppendFailed) {
        }

        self::assertSame(1, $this->eventStore->head()?->value);
    }

    #[Test]
    public function head_does_not_include_events_that_are_not_committed_yet(): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
            self::markTestSkipped('SQLite only has a single writer');
        }
        $this->eventStore->append(Event::create('SomeEvent', 'a'));

        $otherConnection = self::createConnection();
        $otherConnection->beginTransaction();
        $otherConnection->executeStatement('INSERT INTO ' . self::TABLE . " (type, data, metadata, tags, recorded_at) VALUES ('SomeEvent', 'b', '{}', '[]', '2026-01-01 00:00:00')");
        try {
            self::assertSame(1, $this->eventStore->head()?->value);
        } finally {
            $otherConnection->rollBack();
            $otherConnection->close();
        }
    }

    #[Test]
    public function readPartition_throws_on_sqlite(): void
    {
        if (!$this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
            self::markTestSkipped('Only SQLite does not support partitioned reads');
        }
        $this->expectException(RuntimeException::class);
        $this->eventStore->readPartition(TagPartition::create('cart:', 1, 0), Query::all());
    }

    #[Test]
    public function readPartition_returns_the_same_events_as_the_php_implementation(): void
    {
        $this->skipOnSQLite();
        $this->appendFixture();

        foreach ([1, 2, 7, 8, 1000] as $partitions) {
            foreach ([0, 1, $partitions - 1] as $number) {
                if ($number >= $partitions) {
                    continue;
                }
                $partition = TagPartition::create('cart:', $partitions, $number);
                $expected = [];
                foreach ($this->eventStore->read(Query::all()) as $sequencedEvent) {
                    foreach ($sequencedEvent->event->tags as $tag) {
                        if ($partition->contains($tag)) {
                            $expected[] = $sequencedEvent->position->value;
                            break;
                        }
                    }
                }
                self::assertSame($expected, $this->positions($partition), sprintf('partition %d of %d', $number, $partitions));
            }
        }
    }

    #[Test]
    public function readPartition_implements_the_frozen_hash_contract(): void
    {
        $this->skipOnSQLite();
        // see TagPartitionTest for the origin of these numbers
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'b', ['cart:abc-123']));

        self::assertSame([1], $this->positions(TagPartition::create('cart:', 8, 0)));
        self::assertSame([2], $this->positions(TagPartition::create('cart:', 8, 3)));
        self::assertSame([1], $this->positions(TagPartition::create('cart:', 1000, 424)));
        self::assertSame([2], $this->positions(TagPartition::create('cart:', 1000, 243)));
    }

    #[Test]
    public function readPartition_supports_the_highest_possible_number_of_partitions(): void
    {
        $this->skipOnSQLite();
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'b', ['cart:abc-123']));

        self::assertSame([1], $this->positions(TagPartition::create('cart:', 2 ** 32, 1355443424)));
        self::assertSame([2], $this->positions(TagPartition::create('cart:', 2 ** 32, 2954419243)));
    }

    /**
     * @return iterable<array{int}>
     */
    public static function partitionCounts(): iterable
    {
        foreach ([1, 2, 3, 4, 5, 8] as $partitions) {
            yield 'n = ' . $partitions => [$partitions];
        }
    }

    #[Test]
    #[DataProvider('partitionCounts')]
    public function doubling_the_number_of_partitions_splits_partition_k_into_k_and_k_plus_n(int $n): void
    {
        $this->skipOnSQLite();
        $this->appendFixture();

        for ($k = 0; $k < $n; $k++) {
            $union = array_values(array_unique([
                ...$this->positions(TagPartition::create('cart:', 2 * $n, $k)),
                ...$this->positions(TagPartition::create('cart:', 2 * $n, $k + $n)),
            ]));
            sort($union);
            self::assertNotEmpty($union, sprintf('partition %d of %d', $k, $n));
            self::assertSame($this->positions(TagPartition::create('cart:', $n, $k)), $union, sprintf('partition %d of %d', $k, $n));
        }
    }

    #[Test]
    public function events_with_a_single_matching_tag_are_in_exactly_one_partition(): void
    {
        $this->skipOnSQLite();
        for ($i = 0; $i < 100; $i++) {
            $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:' . $i, 'product:' . $i]));
        }

        foreach ([2, 3, 8] as $partitions) {
            $all = [];
            for ($number = 0; $number < $partitions; $number++) {
                $all = [...$all, ...$this->positions(TagPartition::create('cart:', $partitions, $number))];
            }
            sort($all);
            self::assertSame(range(1, 100), $all);
        }
    }

    #[Test]
    public function events_with_several_matching_tags_are_in_every_owning_partition(): void
    {
        $this->skipOnSQLite();
        // at n = 8 "cart:1" belongs to partition 0 and "cart:abc-123" to partition 3
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1', 'cart:abc-123']));

        for ($number = 0; $number < 8; $number++) {
            $expected = in_array($number, [0, 3], true) ? [1] : [];
            self::assertSame($expected, $this->positions(TagPartition::create('cart:', 8, $number)), 'partition ' . $number);
        }
    }

    #[Test]
    public function partitions_cover_exactly_the_events_with_a_matching_tag(): void
    {
        $this->skipOnSQLite();
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'b', ['product:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'c'));
        $this->eventStore->append(Event::create('SomeEvent', 'd', ['product:2', 'cart:2']));

        self::assertSame([1, 4], $this->positions(TagPartition::create('cart:', 1, 0)));
        $all = [];
        for ($number = 0; $number < 8; $number++) {
            $all = [...$all, ...$this->positions(TagPartition::create('cart:', 8, $number))];
        }
        sort($all);
        self::assertSame([1, 4], $all);
    }

    #[Test]
    public function the_prefix_is_matched_literally_and_case_sensitively(): void
    {
        $this->skipOnSQLite();
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['my_cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'b', ['myXcart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'c', ['Cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'd', ['cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'e', ['xcart:1']));

        self::assertSame([1], $this->positions(TagPartition::create('my_cart:', 1, 0)));
        self::assertSame([4], $this->positions(TagPartition::create('cart:', 1, 0)));
        self::assertSame([3], $this->positions(TagPartition::create('Cart:', 1, 0)));
    }

    #[Test]
    public function partitions_are_roughly_balanced(): void
    {
        $this->skipOnSQLite();
        $this->appendFixture();

        $sizes = [];
        for ($number = 0; $number < 4; $number++) {
            $sizes[] = count($this->positions(TagPartition::create('product:', 4, $number)));
        }
        // 400 events with distinct tags in 4 partitions. The events are fixed, so this can't flake
        self::assertSame(400, array_sum($sizes));
        self::assertGreaterThan(50, min($sizes));
        self::assertLessThan(150, max($sizes));
    }

    #[Test]
    public function the_partition_is_combined_with_the_query(): void
    {
        $this->skipOnSQLite();
        $this->eventStore->append(Event::create('TypeA', 'a', ['cart:1', 'product:1']));
        $this->eventStore->append(Event::create('TypeB', 'b', ['cart:1', 'product:2']));
        $this->eventStore->append(Event::create('TypeC', 'c', ['cart:1', 'product:3']));
        $this->eventStore->append(Event::create('TypeA', 'd', ['product:1']));
        $partition = TagPartition::create('cart:', 1, 0);

        self::assertSame([1], $this->positions($partition, Query::fromItems(QueryItem::create(eventTypes: 'TypeA'))));
        self::assertSame([2], $this->positions($partition, Query::fromItems(QueryItem::create(tags: 'product:2'))));
        self::assertSame([1, 3], $this->positions($partition, Query::fromItems(
            QueryItem::create(eventTypes: 'TypeA'),
            QueryItem::create(eventTypes: 'TypeC', tags: 'product:3'),
        )));
    }

    #[Test]
    public function readPartition_rejects_query_items_with_onlyLastEvent(): void
    {
        $this->skipOnSQLite();
        $this->expectException(InvalidArgumentException::class);
        $this->eventStore->readPartition(TagPartition::create('cart:', 1, 0), Query::fromItems(QueryItem::create(eventTypes: 'SomeEvent', onlyLastEvent: true)));
    }

    #[Test]
    public function readPartition_respects_read_options_and_until(): void
    {
        $this->skipOnSQLite();
        for ($i = 0; $i < 5; $i++) {
            $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1']));
        }
        $partition = TagPartition::create('cart:', 1, 0);

        self::assertSame([2, 3, 4, 5], $this->positions($partition, options: ReadOptions::create(from: 2)));
        self::assertSame([1, 2], $this->positions($partition, options: ReadOptions::create(limit: 2)));
        self::assertSame([5, 4], $this->positions($partition, options: ReadOptions::create(backwards: true, limit: 2)));
        self::assertSame([1, 2, 3], $this->positions($partition, until: SequencePosition::fromInteger(3)));
        self::assertSame([2, 3], $this->positions($partition, options: ReadOptions::create(from: 2), until: SequencePosition::fromInteger(3)));
        self::assertSame([3], $this->positions($partition, options: ReadOptions::create(from: 3), until: SequencePosition::fromInteger(3)));
    }

    #[Test]
    public function readPartition_rejects_until_when_reading_backwards(): void
    {
        $this->skipOnSQLite();
        $this->expectException(InvalidArgumentException::class);
        $this->eventStore->readPartition(TagPartition::create('cart:', 1, 0), Query::all(), ReadOptions::create(backwards: true), SequencePosition::fromInteger(1));
    }

    #[Test]
    public function documented_worker_loop_reads_every_event_once_and_advances_without_hits(): void
    {
        $this->skipOnSQLite();
        $partition = TagPartition::create('cart:', 1, 0);
        $this->eventStore->append(Event::create('SomeEvent', 'a', ['cart:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'b', ['cart:2']));
        $this->eventStore->append(Event::create('SomeEvent', 'c', ['cart:3']));
        $this->eventStore->append(Event::create('SomeEvent', 'd', ['product:1']));
        $this->eventStore->append(Event::create('SomeEvent', 'e', ['product:2']));

        $handled = [];
        $checkpoint = SequencePosition::fromInteger(0);
        $head = $this->eventStore->head();
        self::assertNotNull($head);
        for ($poll = 0; $poll < 3; $poll++) {
            $numberOfEvents = 0;
            foreach ($this->eventStore->readPartition($partition, Query::all(), ReadOptions::create(from: $checkpoint->next(), limit: 2), $head) as $sequencedEvent) {
                $handled[] = $sequencedEvent->position->value;
                $checkpoint = $sequencedEvent->position;
                $numberOfEvents++;
            }
            if ($numberOfEvents < 2) {
                $checkpoint = $head;
            }
        }

        self::assertSame([1, 2, 3], $handled);
        self::assertSame(5, $checkpoint->value);
    }

    private function skipOnSQLite(): void
    {
        if ($this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
            self::markTestSkipped('SQLite does not support partitioned reads');
        }
    }

    /**
     * 400 events with distinct "product:" tags, 300 of them with a "cart:" tag and 40 of those with two.
     * Some other events have no matching tag, no tag at all or a similar tag
     */
    private function appendFixture(): void
    {
        $events = [];
        for ($i = 0; $i < 400; $i++) {
            $tags = ['product:' . $i];
            if ($i % 4 !== 3) {
                $tags[] = 'cart:' . ($i % 150);
            }
            if ($i % 10 === 0) {
                $tags[] = 'cart:' . (1000 + $i);
            }
            $events[] = Event::create($i % 2 === 0 ? 'TypeA' : 'TypeB', 'data', $tags);
            if (count($events) === 100) {
                $this->eventStore->append(Events::fromArray($events));
                $events = [];
            }
        }
        $this->eventStore->append(Events::fromArray([
            Event::create('TypeA', 'no tag'),
            Event::create('TypeA', 'other tag', ['user:1']),
            Event::create('TypeA', 'similar tag', ['Cart:1']),
            Event::create('TypeA', 'similar tag', ['xcart:1']),
        ]));
    }

    /**
     * @return list<int>
     */
    private function positions(TagPartition $partition, Query|null $query = null, ReadOptions|null $options = null, SequencePosition|null $until = null): array
    {
        $positions = [];
        foreach ($this->eventStore->readPartition($partition, $query ?? Query::all(), $options, $until) as $sequencedEvent) {
            $positions[] = $sequencedEvent->position->value;
        }
        return $positions;
    }

    private static function createConnection(): Connection
    {
        $dsn = getenv('DCB_TEST_DSN');
        if (!is_string($dsn)) {
            // doctrine/dbal 3.6.0 fails to parse the equivalent "pdo-sqlite:///events_test.sqlite"
            $dsn = 'pdo-sqlite://localhost/events_test.sqlite';
        }
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection((new DsnParser())->parse($dsn), $config);
    }
}
