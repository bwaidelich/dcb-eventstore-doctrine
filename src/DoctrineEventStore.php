<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\DeadlockException;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaDiff;
use Doctrine\DBAL\Schema\SchemaException;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Exception;
use JsonException;
use RuntimeException;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Setupable;
use Wwwision\DCBEventStore\Types\AppendCondition;
use Wwwision\DCBEventStore\Types\Event;
use Wwwision\DCBEventStore\Types\EventEnvelope;
use Wwwision\DCBEventStore\Types\EventEnvelopes;
use Wwwision\DCBEventStore\Types\Events;
use Wwwision\DCBEventStore\Types\ReadOptions;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBEventStore\Types\StreamQuery\CriterionHashes;
use Wwwision\DCBEventStore\Types\StreamQuery\StreamQuery;
use function implode;
use function json_encode;
use function sprintf;
use const JSON_THROW_ON_ERROR;

final class DoctrineEventStore implements EventStore, Setupable
{

    public function __construct(
        private readonly DoctrineEventStoreConfiguration $config
    ) {
    }

    public static function create(Connection $connection, string $eventTableName, CriterionImplementations $criterionImplementations = null): self
    {
        $config = DoctrineEventStoreConfiguration::create($connection, $eventTableName, $criterionImplementations);
        return new self($config);
    }

    public function setup(): void
    {
        try {
            // TODO find replacement, @see https://github.com/doctrine/dbal/blob/3.6.x/UPGRADE.md#deprecated-schemadifftosql-and-schemadifftosavesql
            foreach ($this->getSchemaDiff()->toSaveSql($this->config->platform) as $statement) {
                $this->config->connection->executeStatement($statement);
            }
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to setup event store: %s', $e->getMessage()), 1687010035, $e);
        }
    }

    private function getSchemaDiff(): SchemaDiff
    {
        if (method_exists($this->config->connection, 'createSchemaManager')) {
            $schemaManager = $this->config->connection->createSchemaManager();
            return $schemaManager->createComparator()->compareSchemas($schemaManager->introspectSchema(), $this->databaseSchema());
        }
        $schemaManager = $this->config->connection->getSchemaManager();
        assert($schemaManager !== null);
        return (new Comparator())->compare($schemaManager->createSchema(), $this->databaseSchema());
    }

    /**
     * @throws SchemaException
     */
    private function databaseSchema(): Schema
    {
        $schema = new Schema();
        $table = $schema->createTable($this->config->eventTableName);
        // The monotonic sequence number
        $table->addColumn('sequence_number', Types::INTEGER, ['autoincrement' => true]);
        // The unique event id, usually a UUID
        $table->addColumn('id', Types::STRING, ['length' => 255]);
        // The event type in the format "<BoundedContext>:<EventType>"
        $table->addColumn('type', Types::STRING, ['length' => 255]);
        // The event payload (usually serialized as JSON)
        $table->addColumn('data', Types::TEXT);
        // Optional event metadata as key-value pairs
        $table->addColumn('metadata', Types::TEXT, ['notnull' => false, 'platformOptions' => ['jsonb' => true]]);
        // The event tags (aka domain ids) as JSON
        $table->addColumn('tags', Types::JSON, ['platformOptions' => ['jsonb' => true]]);
        // When the event was appended originally
        $table->addColumn('recorded_at', Types::DATETIME_IMMUTABLE);

        $table->setPrimaryKey(['sequence_number']);
        $table->addUniqueIndex(['id']);
        return $schema;
    }

    public function read(StreamQuery $query, ?ReadOptions $options = null): EventStream
    {
        if ($query->isWildcard()) {
            return $this->readAll($options);
        }
        $backwards = $options?->backwards ?? false;
        $queryBuilder = $this->config->connection->createQueryBuilder()
            ->select('events.*, criterion_hashes')
            ->from($this->config->eventTableName, 'events')
            ->orderBy('events.sequence_number', $backwards ? 'DESC' : 'ASC');
        if ($options !== null && $options->from !== null) {
            $operator = $backwards ? '<=' : '>=';
            $queryBuilder->andWhere('events.sequence_number ' . $operator . ' :minimumSequenceNumber')->setParameter('minimumSequenceNumber', $options->from->value);
        }
        $this->addStreamQueryConstraints($queryBuilder, $query);
        return self::toEventStream($queryBuilder);
    }

    public function readAll(?ReadOptions $options = null): EventStream
    {
        $backwards = $options?->backwards ?? false;
        $queryBuilder = $this->config->connection->createQueryBuilder()
            ->select('events.*, \'\' AS criterion_hashes')
            ->from($this->config->eventTableName, 'events')
            ->orderBy('events.sequence_number', $backwards ? 'DESC' : 'ASC');
        if ($options !== null && $options->from !== null) {
            $operator = $backwards ? '<=' : '>=';
            $queryBuilder->andWhere('events.sequence_number ' . $operator . ' :minimumSequenceNumber')->setParameter('minimumSequenceNumber', $options->from->value);
        }
        return self::toEventStream($queryBuilder);
    }

    private static function toEventStream(QueryBuilder $queryBuilder): DoctrineEventStream
    {
        if (method_exists($queryBuilder, 'executeQuery')) {
            return new DoctrineEventStream($queryBuilder->executeQuery());
        }
        $result = $queryBuilder->execute();
        assert($result instanceof Result);
        return new DoctrineEventStream($result);
    }

    public function append(Events $events, AppendCondition $condition): void
    {
        try {
            $this->reconnectDatabaseConnection();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to commit events because database connection could not be reconnected: %s', $e->getMessage()), 1685956292, $e);
        }
        Assert::eq($this->config->connection->getTransactionNestingLevel(), 0, 'Failed to commit events because a database transaction is active already');

        $parameters = [];
        $selects = [];
        $eventIndex = 0;
        $now = $this->config->clock->now();
        foreach ($events as $event) {
            $selects[] = "SELECT :e{$eventIndex}_id id, :e{$eventIndex}_type type, :e{$eventIndex}_data data, :e{$eventIndex}_metadata metadata, :e{$eventIndex}_tags" . ($this->config->isPostgreSQL() ? '::jsonb' : '') . " tags, :e{$eventIndex}_recordedAt" . ($this->config->isPostgreSQL() ? '::timestamp' : '') . " recorded_at";
            try {
                $tags = json_encode($event->tags, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new RuntimeException(sprintf('Failed to JSON encode tags: %s', $e->getMessage()), 1686304410, $e);
            }
            $parameters['e' . $eventIndex . '_id'] = $event->id->value;
            $parameters['e' . $eventIndex . '_type'] = $event->type->value;
            $parameters['e' . $eventIndex . '_data'] = $event->data->value;
            $parameters['e' . $eventIndex . '_metadata'] = json_encode($event->metadata->value, JSON_THROW_ON_ERROR);
            $parameters['e' . $eventIndex . '_tags'] = $tags;
            $parameters['e' . $eventIndex . '_recordedAt'] = $now->format('Y-m-d H:i:s');
            $eventIndex++;
        }
        $unionSelects = implode(' UNION ALL ', $selects);

        $statement = "INSERT INTO {$this->config->eventTableName} (id, type, data, metadata, tags, recorded_at) SELECT * FROM ( $unionSelects ) new_events";
        $queryBuilder = null;
        if (!$condition->expectedHighestSequenceNumber->isAny()) {
            $queryBuilder = $this->config->connection->createQueryBuilder()->select('events.sequence_number')->from($this->config->eventTableName, 'events')->orderBy('events.sequence_number', 'DESC')->setMaxResults(1);
            $this->addStreamQueryConstraints($queryBuilder, $condition->query);
            $parameters = [...$parameters, ...$queryBuilder->getParameters()];
            if ($condition->expectedHighestSequenceNumber->isNone()) {
                $statement .= ' WHERE NOT EXISTS (' . $queryBuilder->getSQL() . ')';
            } else {
                $statement .= ' WHERE (' . $queryBuilder->getSQL() . ') = :highestSequenceNumber';
                $parameters['highestSequenceNumber'] = $condition->expectedHighestSequenceNumber->extractSequenceNumber()->value;
            }
        }
        $affectedRows = $this->commitStatement($statement, $parameters, $queryBuilder?->getParameterTypes() ?? []);
        if ($affectedRows === 0 && !$condition->expectedHighestSequenceNumber->isAny()) {
            throw $condition->expectedHighestSequenceNumber->isNone() ? ConditionalAppendFailed::becauseNoEventWhereExpected() : ConditionalAppendFailed::becauseHighestExpectedSequenceNumberDoesNotMatch($condition->expectedHighestSequenceNumber);
        }
    }

    // -------------------------------------

    /**
     * @param array<int|string, mixed> $parameters
     * @param array<int|string, Type|int|string|null> $parameterTypes
     */
    private function commitStatement(string $statement, array $parameters, array $parameterTypes): int
    {
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 10;
        $retryAttempt = 0;
        while (true) {
            try {
                if ($this->config->isPostgreSQL()) {
                    $this->config->connection->executeStatement('BEGIN ISOLATION LEVEL SERIALIZABLE');
                }
                $affectedRows = (int)$this->config->connection->executeStatement($statement, $parameters, $parameterTypes);
                if ($this->config->isPostgreSQL()) {
                    $this->config->connection->executeStatement('COMMIT');
                }
                return $affectedRows;
            } catch (DeadlockException $e) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    throw new RuntimeException(sprintf('Failed after %d retry attempts', $retryAttempt), 1686565685, $e);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt ++;
                $retryWaitInterval *= 2;
            } catch (DbalException $e) {
                throw new RuntimeException(sprintf('Failed to commit events (error code: %d): %s', (int)$e->getCode(), $e->getMessage()), 1685956215, $e);
            } finally {
                if ($this->config->isPostgreSQL()) {
                    $this->config->connection->executeStatement('ROLLBACK');
                }
            }
        }
    }

    private function addStreamQueryConstraints(QueryBuilder $queryBuilder, StreamQuery $streamQuery): void
    {
        if ($streamQuery->isWildcard()) {
            return;
        }
        $this->config->resetUniqueParameterCount();
        $criterionStatements = [];
        $processedCriterionHashes = [];
        foreach ($streamQuery->criteria as $criterion) {
            $hash = $criterion->hash()->value;
            if (in_array($hash, $processedCriterionHashes, true)) {
                continue;
            }
            $processedCriterionHashes[] = $hash;
            $criterionQueryBuilder = $this->config->connection->createQueryBuilder()
                ->select('sequence_number, \'' . $hash . '\' AS criterion_hash')
                ->from($this->config->eventTableName, 'events');
            $this->config->criterionImplementations->applyCriterionConstraints($criterion, $criterionQueryBuilder, $this->config);
            $criterionStatements[] = $criterionQueryBuilder->getSQL();
            $queryBuilder->setParameters([...$queryBuilder->getParameters(), ...$criterionQueryBuilder->getParameters()], [...$queryBuilder->getParameterTypes(), ...$criterionQueryBuilder->getParameterTypes()]);
        }
        $joinQueryBuilder = $this->config->connection->createQueryBuilder()
            ->select('sequence_number')
            ->from('(' . implode(' UNION ALL ', $criterionStatements) . ')', 'h')
            ->groupBy('h.sequence_number');
        if ($this->config->isPostgreSQL()) {
            $joinQueryBuilder->addSelect('STRING_AGG(criterion_hash, \',\') AS criterion_hashes');
        } else {
            $joinQueryBuilder->addSelect('GROUP_CONCAT(criterion_hash) AS criterion_hashes');
        }
        $queryBuilder->innerJoin('events', '(' . $joinQueryBuilder->getSQL() . ')', 'eh', 'eh.sequence_number = events.sequence_number');
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->config->connection->fetchOne('SELECT 1');
        } catch (Exception $_) {
            $this->config->connection->close();
            try {
                $this->config->connection->connect();
            } catch (DbalException $e) {
                throw new RuntimeException(sprintf('Failed to reconnect database connection: %s', $e->getMessage()), 1686045084, $e);
            }
        }
    }
}
