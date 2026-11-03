<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

use function array_filter;
use function array_keys;
use function array_map;
use function array_values;
use function getenv;
use function is_string;
use function iterator_to_array;
use function str_ends_with;

#[CoversClass(DoctrineEventStore::class)]
final class SetupTest extends TestCase
{
    private const TABLE_A = 'dcb_setup_test_a';
    private const TABLE_B = 'dcb_setup_test_b';
    private const SCHEMA = 'dcb_setup_test';

    private Connection $connection;

    protected function setUp(): void
    {
        $dsn = getenv('DCB_TEST_DSN');
        if (!is_string($dsn)) {
            $dsn = 'pdo-sqlite://localhost/events_test.sqlite';
        }
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        $this->connection = DriverManager::getConnection((new DsnParser())->parse($dsn), $config);
        $this->dropTables();
    }

    protected function tearDown(): void
    {
        $this->dropTables();
        $this->connection->close();
    }

    #[Test]
    public function setup_supports_multiple_event_tables_on_the_same_connection(): void
    {
        $eventStoreA = DoctrineEventStore::create($this->connection, self::TABLE_A);
        $eventStoreB = DoctrineEventStore::create($this->connection, self::TABLE_B);

        $eventStoreA->setup();
        $eventStoreB->setup();

        $eventStoreA->append(Event::create('SomeEvent', 'a', [], []));
        $eventStoreB->append(Event::create('SomeEvent', 'b', [], []));

        self::assertSame('a', iterator_to_array($eventStoreA->read(Query::all()))[0]->event->data->value);
        self::assertSame('b', iterator_to_array($eventStoreB->read(Query::all()))[0]->event->data->value);

        $expectedIndexCount = $this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform ? 4 : 3;
        self::assertCount($expectedIndexCount, $this->indexNames(self::TABLE_A));
        self::assertCount($expectedIndexCount, $this->indexNames(self::TABLE_B));
    }

    #[Test]
    public function setup_creates_a_tags_index_for_each_postgres_event_table(): void
    {
        $this->skipUnlessPostgreSql();
        DoctrineEventStore::create($this->connection, self::TABLE_A)->setup();
        DoctrineEventStore::create($this->connection, self::TABLE_B)->setup();

        self::assertCount(1, self::namesEndingWith($this->indexNames(self::TABLE_A), '_tags'));
        self::assertCount(1, self::namesEndingWith($this->indexNames(self::TABLE_B), '_tags'));
    }

    #[Test]
    public function setup_is_repeatable(): void
    {
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE_A);

        $eventStore->setup();
        $indexNames = $this->indexNames(self::TABLE_A);
        $eventStore->setup();

        self::assertEqualsCanonicalizing($indexNames, $this->indexNames(self::TABLE_A));
    }

    #[Test]
    public function setup_renames_indexes_of_an_existing_table(): void
    {
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE_A);
        $eventStore->setup();
        $expectedIndexNames = $this->indexNames(self::TABLE_A);
        $this->replaceSecondaryIndexes(self::TABLE_A, createFormerIndexes: true);
        self::assertContains('dcb_setup_test_former_type', $this->indexNames(self::TABLE_A));

        $eventStore->setup();

        self::assertEqualsCanonicalizing($expectedIndexNames, $this->indexNames(self::TABLE_A));
    }

    #[Test]
    public function setup_restores_indexes_of_a_table_left_behind_by_a_failed_setup(): void
    {
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE_A);
        $eventStore->setup();
        $expectedIndexNames = $this->indexNames(self::TABLE_A);
        $this->replaceSecondaryIndexes(self::TABLE_A, createFormerIndexes: false);

        $eventStore->setup();

        self::assertEqualsCanonicalizing($expectedIndexNames, $this->indexNames(self::TABLE_A));
    }

    #[Test]
    public function setup_does_not_rebuild_the_tags_index(): void
    {
        $this->skipUnlessPostgreSql();
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE_A);
        $eventStore->setup();
        $indexOid = $this->tagsIndexOid(self::TABLE_A);

        $eventStore->setup();

        self::assertSame($indexOid, $this->tagsIndexOid(self::TABLE_A));
    }

    #[Test]
    public function setup_renames_the_former_tags_index_without_rebuilding_it(): void
    {
        $this->skipUnlessPostgreSql();
        // separate schema, because other tables of the test database might still own an index named "tags"
        $tableName = self::SCHEMA . '.' . self::TABLE_A;
        $this->connection->executeStatement('DROP SCHEMA IF EXISTS ' . self::SCHEMA . ' CASCADE');
        $this->connection->executeStatement('CREATE SCHEMA ' . self::SCHEMA);
        try {
            $eventStore = DoctrineEventStore::create($this->connection, $tableName);
            $eventStore->setup();
            $expectedIndexName = $this->tagsIndexName($tableName);
            $this->connection->executeStatement('DROP INDEX ' . self::SCHEMA . '.' . $expectedIndexName);
            $this->connection->executeStatement("CREATE INDEX tags ON $tableName USING gin (tags jsonb_path_ops)");
            $indexOid = $this->tagsIndexOid($tableName);

            $eventStore->setup();

            self::assertSame($expectedIndexName, $this->tagsIndexName($tableName));
            self::assertSame($indexOid, $this->tagsIndexOid($tableName));
        } finally {
            $this->connection->executeStatement('DROP SCHEMA ' . self::SCHEMA . ' CASCADE');
        }
    }

    // -------------------------------------

    private function skipUnlessPostgreSql(): void
    {
        if (!$this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            self::markTestSkipped('The GIN index on tags only exists on PostgreSQL');
        }
    }

    private function tagsIndexOid(string $tableName): string
    {
        return $this->fetchTagsIndexColumn($tableName, 'i.indexrelid::text');
    }

    private function tagsIndexName(string $tableName): string
    {
        return $this->fetchTagsIndexColumn($tableName, 'c.relname');
    }

    private function fetchTagsIndexColumn(string $tableName, string $column): string
    {
        $value = $this->connection->fetchOne(
            "SELECT $column FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am a ON a.oid = c.relam WHERE i.indrelid = ?::regclass AND a.amname = 'gin'",
            [$tableName],
        );
        self::assertIsString($value);
        return $value;
    }

    private function replaceSecondaryIndexes(string $tableName, bool $createFormerIndexes): void
    {
        $schemaManager = $this->connection->createSchemaManager();
        foreach ($this->indexNames($tableName) as $indexName) {
            if ($indexName !== 'primary') {
                $schemaManager->dropIndex($indexName, $tableName);
            }
        }
        if (!$createFormerIndexes) {
            return;
        }
        // names are unique to this test, because other tables of the test database might still own "idx_type" & co.
        $this->connection->executeStatement("CREATE INDEX dcb_setup_test_former_type ON $tableName (type)");
        $this->connection->executeStatement("CREATE INDEX dcb_setup_test_former_type_seq ON $tableName (type, sequence_number)");
        if ($this->connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $this->connection->executeStatement("CREATE INDEX dcb_setup_test_former_tags ON $tableName USING gin (tags jsonb_path_ops)");
        }
    }

    /**
     * @return list<string>
     */
    private function indexNames(string $tableName): array
    {
        return array_map('strval', array_keys($this->connection->createSchemaManager()->listTableIndexes($tableName)));
    }

    /**
     * @param list<string> $names
     * @return list<string>
     */
    private static function namesEndingWith(array $names, string $suffix): array
    {
        return array_values(array_filter($names, static fn(string $name) => str_ends_with($name, $suffix)));
    }

    private function dropTables(): void
    {
        foreach ([self::TABLE_A, self::TABLE_B] as $tableName) {
            $this->connection->executeStatement("DROP TABLE IF EXISTS $tableName");
        }
    }
}
