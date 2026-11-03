<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use DateTimeImmutable;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use InvalidArgumentException;
use JsonException;
use RuntimeException;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\AppendCondition\AppendCondition;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\Event\EventType;
use Wwwision\DCBEventStore\Event\Tag;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\Query\QueryItem;
use Wwwision\DCBEventStore\ReadOptions;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvents;

use Wwwision\DCBEventStore\SequencedEvent\SequencePosition;

use function count;
use function hash;
use function implode;
use function iterator_to_array;
use function json_encode;
use function sprintf;
use function strlen;
use function strrpos;
use function substr;

use const JSON_THROW_ON_ERROR;

final class DoctrineEventStore implements EventStore
{
    private const FORMER_TAGS_INDEX_NAME = 'tags';

    public function __construct(
        private readonly DoctrineEventStoreConfiguration $config,
    ) {}

    public static function create(Connection $connection, string $eventTableName): self
    {
        $config = DoctrineEventStoreConfiguration::create($connection, $eventTableName);
        return new self($config);
    }

    public function setup(): void
    {
        try {
            foreach ($this->determineRequiredSqlStatements() as $statement) {
                $this->config->connection->executeStatement($statement);
            }
            if ($this->config->isPostgreSQL()) {
                $this->config->connection->executeStatement('CREATE INDEX IF NOT EXISTS ' . $this->indexName('tags') . ' ON ' . $this->config->eventTableName . ' USING gin (tags jsonb_path_ops)');
            }
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to setup event store: %s', $e->getMessage()), 1687010035, $e);
        }
    }

    /**
     * @return array<string>
     * @throws DbalException
     */
    private function determineRequiredSqlStatements(): array
    {
        $schemaManager = $this->config->connection->createSchemaManager();
        $platform = $this->config->connection->getDatabasePlatform();
        if (!$schemaManager->tablesExist([$this->config->eventTableName])) {
            return $platform->getCreateTableSQL($this->databaseSchema($schemaManager)->getTable($this->config->eventTableName));
        }
        $tableSchema = $schemaManager->introspectTable($this->config->eventTableName);
        $tagsIndexStatements = $this->config->isPostgreSQL() ? $this->removeTagsIndexFromComparison($tableSchema) : [];
        $fromSchema = new Schema([$tableSchema], [], $schemaManager->createSchemaConfig());
        $schemaDiff = $schemaManager->createComparator()->compareSchemas($fromSchema, $this->databaseSchema($schemaManager));
        return [...$tagsIndexStatements, ...$platform->getAlterSchemaSQL($schemaDiff)];
    }

    /**
     * The GIN index on tags can't be expressed in the DBAL schema, so the comparator would drop it.
     * Removing it from the comparison prevents setup() from rebuilding it, which can take long and blocks appends.
     *
     * @return list<string> statements that are required to keep the index
     */
    private function removeTagsIndexFromComparison(Table $tableSchema): array
    {
        $indexName = $this->indexName('tags');
        if ($tableSchema->hasIndex($indexName)) {
            $tableSchema->dropIndex($indexName);
            return [];
        }
        if (!$tableSchema->hasIndex(self::FORMER_TAGS_INDEX_NAME)) {
            return [];
        }
        $tableSchema->dropIndex(self::FORMER_TAGS_INDEX_NAME);
        $schemaSeparatorPosition = strrpos($this->config->eventTableName, '.');
        $schemaPrefix = $schemaSeparatorPosition === false ? '' : substr($this->config->eventTableName, 0, $schemaSeparatorPosition + 1);
        return ['ALTER INDEX ' . $schemaPrefix . self::FORMER_TAGS_INDEX_NAME . ' RENAME TO ' . $indexName];
    }

    /**
     * @param AbstractSchemaManager<AbstractPlatform> $schemaManager
    */
    private function databaseSchema(AbstractSchemaManager $schemaManager): Schema
    {
        $schemaConfiguration = $schemaManager->createSchemaConfig();
        $defaultTableOptions = $schemaConfiguration->getDefaultTableOptions();
        if (!isset($defaultTableOptions['charset']) && !$this->config->isSQLite()) {
            $defaultTableOptions['charset'] = 'utf8mb4';
            $schemaConfiguration->setDefaultTableOptions($defaultTableOptions);
        }
        $schema = new Schema(schemaConfig: $schemaConfiguration);

        $eventsTable = $schema->createTable($this->config->eventTableName);
        $eventsTable->addColumn('sequence_number', $this->config->isSQLite() ? Types::INTEGER : Types::BIGINT)
            ->setAutoincrement(true)
            ->setUnsigned(true);

        $eventsTable->addColumn('type', Types::STRING)
            ->setLength(EventType::LENGTH_MAX)
            ->setPlatformOptions($this->config->isSQLite() ? [] : ['charset' => 'ascii']);

        $eventsTable->addColumn('data', Types::TEXT);

        $eventsTable->addColumn('metadata', Types::JSON)
            ->setNotnull(false)
            ->setPlatformOptions($this->config->isPostgreSQL() ? ['jsonb' => true] : []);

        $eventsTable->addColumn('tags', Types::JSON)
            ->setPlatformOptions($this->config->isPostgreSQL() ? ['jsonb' => true] : []);

        $eventsTable->addColumn('recorded_at', Types::DATETIME_IMMUTABLE);
        $eventsTable->addIndex(['type'], $this->indexName('type'));
        $eventsTable->addIndex(['type', 'sequence_number'], $this->indexName('type_sequence_number'));
        $eventsTable->setPrimaryKey(['sequence_number']);

        return $schema;
    }

    /**
     * Index names are unique per schema on PostgreSQL and SQLite, so they have to differ between event tables.
     * The table name is hashed to stay within identifier length limits and to support schema-qualified table names.
     */
    private function indexName(string $suffix): string
    {
        return 'idx_' . hash('crc32b', $this->config->eventTableName) . '_' . $suffix;
    }

    public function read(Query $query, ReadOptions|null $options = null): SequencedEvents
    {
        $queryBuilder = $this->createReadQueryBuilder($options);
        if (self::isSingleEventTypeQuery($query)) {
            $this->addDcbQueryWhereConstraints($queryBuilder, $query);
        } else {
            $this->addDcbQueryConstraints($queryBuilder, $query);
        }
        return self::fetchSequencedEvents($queryBuilder);
    }

    /**
     * Returns the events matching the specified {@see Query} that have a tag belonging to the specified {@see TagPartition}
     *
     * An event with multiple matching tags is returned by every partition that owns one of them, an event without a matching tag by none.
     *
     * To avoid rescanning positions without matching events, specify {@see self::head()} as $until. A worker can then continue after
     * the last event if the batch reached the limit, or after $until otherwise. See README.
     *
     * @param SequencePosition|null $until If specified, only events up to and including this position will be returned. Can't be combined with reading backwards
     * @throws RuntimeException if the database platform doesn't support partitioned reads (SQLite)
     * @throws InvalidArgumentException if the $query contains items with "onlyLastEvent" or if $until is combined with reading backwards
     */
    public function readPartition(TagPartition $partition, Query $query, ReadOptions|null $options = null, SequencePosition|null $until = null): SequencedEvents
    {
        if ($this->config->isSQLite()) {
            throw new RuntimeException('Partitioned reads are not supported on SQLite, because it has no built-in hash function', 1793664001);
        }
        foreach ($query as $queryItem) {
            if ($queryItem->onlyLastEvent) {
                throw new InvalidArgumentException('Partitioned reads do not support query items with "onlyLastEvent", because the last event is determined before the partition is applied', 1793664002);
            }
        }
        if ($until !== null && $options !== null && $options->backwards) {
            throw new InvalidArgumentException('Partitioned reads do not support "until" when reading backwards', 1793664003);
        }
        $queryBuilder = $this->createReadQueryBuilder($options);
        $this->addDcbQueryWhereConstraints($queryBuilder, $query);
        if ($until !== null) {
            $queryBuilder->andWhere('events.sequence_number <= :until')->setParameter('until', $until->value, ParameterType::INTEGER);
        }
        $queryBuilder
            ->andWhere($this->partitionConstraint())
            ->setParameter('partitionPrefix', $partition->prefix)
            ->setParameter('partitions', $partition->partitions, ParameterType::INTEGER)
            ->setParameter('partition', $partition->partition, ParameterType::INTEGER);
        if (!$this->config->isPostgreSQL()) {
            $queryBuilder->setParameter('partitionPrefixLength', strlen($partition->prefix), ParameterType::INTEGER);
        }
        return self::fetchSequencedEvents($queryBuilder);
    }

    /**
     * Has to implement the same calculation as {@see TagPartition::contains()}
     */
    private function partitionConstraint(): string
    {
        if ($this->config->isPostgreSQL()) {
            return "EXISTS (SELECT 1 FROM jsonb_array_elements_text(events.tags) partition_tags(tag) WHERE starts_with(partition_tags.tag, :partitionPrefix::text) AND ('x' || substr(encode(sha256(convert_to(partition_tags.tag, 'UTF8')), 'hex'), 1, 8))::bit(32)::bigint % :partitions::bigint = :partition::bigint)";
        }
        // This is not an EXISTS on purpose: MySQL 8 turns that into a semi-join that evaluates JSON_TABLE before the table it depends on and returns no rows
        return "(SELECT 1 FROM JSON_TABLE(events.tags, '$[*]' COLUMNS (tag VARCHAR(" . Tag::LENGTH_MAX . ") PATH '$')) partition_tags WHERE CAST(LEFT(partition_tags.tag, :partitionPrefixLength) AS BINARY) = CAST(:partitionPrefix AS BINARY) AND CAST(CONV(SUBSTR(SHA2(partition_tags.tag, 256), 1, 8), 16, 10) AS UNSIGNED) % :partitions = :partition LIMIT 1) IS NOT NULL";
    }

    /**
     * A query for one event type can walk the index on (type, sequence_number) in the requested order, so it doesn't need a derived table.
     * This doesn't apply to other queries: A database would walk the primary key instead if the filter is expected to match soon,
     * and scan the whole table if the matching events are clustered somewhere else in the log. See docs/decisions.md
     */
    private static function isSingleEventTypeQuery(Query $query): bool
    {
        $items = iterator_to_array($query, false);
        if (count($items) !== 1) {
            return false;
        }
        $item = $items[0];
        return $item->tags === null && !$item->onlyLastEvent && $item->eventTypes !== null && count($item->eventTypes->toStringArray()) === 1;
    }

    private function createReadQueryBuilder(ReadOptions|null $options): QueryBuilder
    {
        $backwards = $options->backwards ?? false;
        $queryBuilder = $this->config->connection->createQueryBuilder()
            ->select('events.*')
            ->from($this->config->eventTableName, 'events')
            ->orderBy('events.sequence_number', $backwards ? 'DESC' : 'ASC');
        if ($options !== null && $options->from !== null) {
            $operator = $backwards ? '<=' : '>=';
            $queryBuilder->andWhere('events.sequence_number ' . $operator . ' :minimumPosition')->setParameter('minimumPosition', $options->from->value);
        }
        if ($options !== null && $options->limit !== null) {
            $queryBuilder->setMaxResults($options->limit);
        }
        return $queryBuilder;
    }

    private static function fetchSequencedEvents(QueryBuilder $queryBuilder): SequencedEvents
    {
        $result = $queryBuilder->executeQuery();
        return SequencedEvents::create(static function () use ($result) {
            while (($row = $result->fetchAssociative()) !== false) {
                yield self::databaseRowToSequencedEvent($row);
            }
        });
    }

    /**
     * Returns the position of the last committed event, or NULL if the event store is empty
     *
     * NOTE: This must not read the counter of the sequence or AUTO_INCREMENT column: It is ahead of the committed
     * events while an append is in flight, so a worker would checkpoint past events it hasn't seen yet
     */
    public function head(): SequencePosition|null
    {
        $head = $this->config->connection->createQueryBuilder()
            ->select('MAX(sequence_number)')
            ->from($this->config->eventTableName)
            ->fetchOne();
        if ($head === null) {
            return null;
        }
        Assert::numeric($head);
        return SequencePosition::fromInteger((int) $head);
    }

    public function append(Events|Event $events, AppendCondition|null $condition = null): void
    {
        Assert::eq($this->config->connection->getTransactionNestingLevel(), 0, 'Failed to commit events because a database transaction is active already');

        $parameters = [];
        $selects = [];
        $eventIndex = 0;
        $now = $this->config->clock->now();
        if ($events instanceof Event) {
            $events = Events::fromArray([$events]);
        }
        foreach ($events as $event) {
            $selects[] = "SELECT :e{$eventIndex}_type type, :e{$eventIndex}_data data, :e{$eventIndex}_metadata" . ($this->config->isPostgreSQL() ? '::jsonb' : '') . " metadata, :e{$eventIndex}_tags" . ($this->config->isPostgreSQL() ? '::jsonb' : '') . " tags, :e{$eventIndex}_recordedAt" . ($this->config->isPostgreSQL() ? '::timestamp' : '') . ' recorded_at';
            try {
                $tags = json_encode($event->tags, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new RuntimeException(sprintf('Failed to JSON encode tags: %s', $e->getMessage()), 1686304410, $e);
            }
            $parameters['e' . $eventIndex . '_type'] = $event->type->value;
            $parameters['e' . $eventIndex . '_data'] = $event->data->value;
            $parameters['e' . $eventIndex . '_metadata'] = json_encode($event->metadata->value, JSON_THROW_ON_ERROR);
            $parameters['e' . $eventIndex . '_tags'] = $tags;
            $parameters['e' . $eventIndex . '_recordedAt'] = $now->format('Y-m-d H:i:s');
            $eventIndex++;
        }
        $unionSelects = implode(' UNION ALL ', $selects);

        $statement = "INSERT INTO {$this->config->eventTableName} (type, data, metadata, tags, recorded_at) SELECT * FROM ( $unionSelects ) new_events";
        $parameterTypes = [];
        if ($condition !== null) {
            $conditionQueryBuilder = $this->config->connection->createQueryBuilder()->select('1')->from($this->config->eventTableName, 'events');
            $this->addDcbQueryConstraints($conditionQueryBuilder, $condition->failIfEventsMatch);
            if ($condition->after !== null) {
                $conditionQueryBuilder->andWhere('events.sequence_number > :highestSequenceNumber');
                $conditionQueryBuilder->setParameter('highestSequenceNumber', $condition->after->value);
            }
            $parameters = [...$parameters, ...$conditionQueryBuilder->getParameters()];
            $parameterTypes = $conditionQueryBuilder->getParameterTypes();
            $statement .= ' WHERE NOT EXISTS (' . $conditionQueryBuilder->getSQL() . ')';
        }
        $affectedRows = $this->commitStatement($statement, $parameters, $parameterTypes);
        if ($affectedRows === 0 && $condition !== null) {
            throw $condition->after === null ? ConditionalAppendFailed::becauseMatchingEventsExist() : ConditionalAppendFailed::becauseMatchingEventsExistAfterSequencePosition($condition->after);
        }
    }

    // -------------------------------------

    /**
     * @param array<int<0, max>|string, mixed> $parameters
     * @param array<int<0, max>|string, ArrayParameterType|ParameterType|Type|string> $parameterTypes
     */
    private function commitStatement(string $statement, array $parameters, array $parameterTypes): int
    {
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 10;
        $retryAttempt = 0;
        while (true) {
            $transactionStarted = false;
            $mysqlAdvisoryLockName = null;
            try {
                if ($this->config->isPostgreSQL()) {
                    // Sequence numbers are allocated at insert time but only become visible on commit. The lock is held
                    // until after the commit, so that a reader can never see a position before all lower ones.
                    // It has to be acquired in READ COMMITTED: In SERIALIZABLE the statement would take the snapshot
                    // before waiting for the lock, and the append condition would miss events committed in the meantime.
                    // See docs/decisions.md
                    $this->config->connection->executeStatement('BEGIN ISOLATION LEVEL READ COMMITTED');
                    $transactionStarted = true;
                    $this->config->connection->executeStatement("SET LOCAL lock_timeout = '30s'");
                    $this->config->connection->fetchOne('SELECT pg_advisory_xact_lock(hashtextextended(?, 0))', ['dcb_' . $this->config->eventTableName]);
                } elseif ($this->config->isMySQL()) {
                    // MySQL's SERIALIZABLE isolation does not acquire gap locks for complex INSERT...WHERE NOT EXISTS
                    // queries with derived subqueries, allowing phantom reads. We use an advisory lock instead to
                    // guarantee mutual exclusion during the check-then-insert operation.
                    $mysqlAdvisoryLockName = 'dcb_' . $this->config->eventTableName;
                    $lockAcquired = $this->config->connection->fetchOne('SELECT GET_LOCK(?, 30)', [$mysqlAdvisoryLockName]);
                    if (!is_numeric($lockAcquired) || (int) $lockAcquired !== 1) {
                        throw new RuntimeException(sprintf('Failed to acquire advisory lock "%s" for event store append', $mysqlAdvisoryLockName));
                    }
                }
                $affectedRows = (int) $this->config->connection->executeStatement($statement, $parameters, $parameterTypes);
                if ($transactionStarted) {
                    $this->config->connection->executeStatement('COMMIT');
                    $transactionStarted = false;
                }
                return $affectedRows;
            } catch (DeadlockException $e) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    throw new RuntimeException(sprintf('Failed after %d retry attempts', $retryAttempt), 1686565685, $e);
                }
                usleep((int) ($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
            } catch (DbalException $e) {
                // MariaDB error 1467 ("Failed to read auto-increment value from storage engine") is a transient
                // concurrency failure under SERIALIZABLE isolation — treat it like a deadlock and retry
                if ((int) $e->getCode() === 1467 && $retryAttempt < $maxRetryAttempts) {
                    usleep((int) ($retryWaitInterval * 1E6));
                    $retryAttempt++;
                    $retryWaitInterval *= 2;
                    continue;
                }
                throw new RuntimeException(sprintf('Failed to commit events (error code: %d): %s', (int) $e->getCode(), $e->getMessage()), 1685956215, $e);
            } finally {
                if ($transactionStarted) {
                    $this->config->connection->executeStatement('ROLLBACK');
                }
                if ($mysqlAdvisoryLockName !== null) {
                    try {
                        $this->config->connection->fetchOne('SELECT RELEASE_LOCK(?)', [$mysqlAdvisoryLockName]);
                    } catch (\Throwable) {
                        // ignore – connection may already be closed
                    }
                }
            }
        }
    }

    private function addDcbQueryConstraints(QueryBuilder $queryBuilder, Query $dcbQuery): void
    {
        if (!$dcbQuery->hasItems()) {
            return;
        }
        $this->config->resetUniqueParameterCount();
        $criterionStatements = [];
        foreach ($dcbQuery as $dcbQueryItem) {
            $criterionQueryBuilder = $this->config->connection->createQueryBuilder()
                ->select('sequence_number')
                ->from($this->config->eventTableName, 'events');
            $this->applyDcbQueryItemConstraints($criterionQueryBuilder, $dcbQueryItem);
            $criterionStatements[] = $criterionQueryBuilder->getSQL();
            $queryBuilder->setParameters([...$queryBuilder->getParameters(), ...$criterionQueryBuilder->getParameters()], [...$queryBuilder->getParameterTypes(), ...$criterionQueryBuilder->getParameterTypes()]);
        }
        $joinQueryBuilder = $this->config->connection->createQueryBuilder()
            ->select('sequence_number')
            ->from('(' . implode(' UNION ALL ', $criterionStatements) . ')', 'h')
            ->groupBy('h.sequence_number');
        $queryBuilder->innerJoin('events', '(' . $joinQueryBuilder->getSQL() . ')', 'eh', 'eh.sequence_number = events.sequence_number');
    }

    /**
     * Alternative to {@see self::addDcbQueryConstraints()} for queries without "onlyLastEvent" that doesn't need a derived table.
     * That table is materialized with all matching events regardless of the requested position and limit.
     */
    private function addDcbQueryWhereConstraints(QueryBuilder $queryBuilder, Query $dcbQuery): void
    {
        if (!$dcbQuery->hasItems()) {
            return;
        }
        $this->config->resetUniqueParameterCount();
        $itemConditions = [];
        foreach ($dcbQuery as $dcbQueryItem) {
            $itemConditions[] = '(' . implode(' AND ', $this->dcbQueryItemConditions($queryBuilder, $dcbQueryItem)) . ')';
        }
        $queryBuilder->andWhere('(' . implode(' OR ', $itemConditions) . ')');
    }

    private function applyDcbQueryItemConstraints(QueryBuilder $queryBuilder, QueryItem $dcbQueryItem): void
    {
        foreach ($this->dcbQueryItemConditions($queryBuilder, $dcbQueryItem) as $condition) {
            $queryBuilder->andWhere($condition);
        }
        if ($dcbQueryItem->onlyLastEvent) {
            $queryBuilder->select('MAX(sequence_number) AS sequence_number');
        }
    }

    /**
     * @return list<string> conditions that have to be combined with AND. The required parameters are added to the $queryBuilder
     */
    private function dcbQueryItemConditions(QueryBuilder $queryBuilder, QueryItem $dcbQueryItem): array
    {
        $conditions = [];
        if ($dcbQueryItem->eventTypes !== null) {
            $eventTypesParameterName = $this->config->createUniqueParameterName();
            $conditions[] = "type IN (:$eventTypesParameterName)";
            $queryBuilder->setParameter($eventTypesParameterName, $dcbQueryItem->eventTypes->toStringArray(), ArrayParameterType::STRING);
        }
        if ($dcbQueryItem->tags !== null) {
            $tagsParameterName = $this->config->createUniqueParameterName();
            if ($this->config->isSQLite()) {
                $conditions[] = "NOT EXISTS(SELECT value FROM JSON_EACH(:$tagsParameterName) WHERE value NOT IN (SELECT value FROM JSON_EACH(tags)))";
            } elseif ($this->config->isPostgreSQL()) {
                $conditions[] = "tags @> :$tagsParameterName::jsonb";
            } else {
                $conditions[] = "JSON_CONTAINS(tags, :$tagsParameterName)";
            }
            $queryBuilder->setParameter($tagsParameterName, json_encode($dcbQueryItem->tags, JSON_THROW_ON_ERROR));
        }
        return $conditions;
    }

    /**
     * @param array<mixed> $row
     */
    private static function databaseRowToSequencedEvent(array $row): SequencedEvent
    {
        Assert::numeric($row['sequence_number']);
        Assert::string($row['recorded_at']);
        Assert::string($row['type']);
        Assert::string($row['data']);
        Assert::string($row['tags']);
        $tagsArray = json_decode($row['tags'], true, 512, JSON_THROW_ON_ERROR);
        Assert::isList($tagsArray);
        Assert::allString($tagsArray);
        Assert::string($row['metadata']);
        $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['recorded_at']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new SequencedEvent(
            SequencePosition::fromInteger((int) $row['sequence_number']),
            $recordedAt,
            Event::create(
                $row['type'],
                $row['data'],
                $tagsArray,
                $row['metadata'],
            ),
        );
    }
}
