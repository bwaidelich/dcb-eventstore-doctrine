<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaException;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Exception;
use JsonException;
use Psr\Clock\ClockInterface;
use RuntimeException;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Exceptions\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Helpers\SystemClock;
use Wwwision\DCBEventStore\Setupable;
use Wwwision\DCBEventStore\Types\AppendCondition;
use Wwwision\DCBEventStore\Types\Events;
use Wwwision\DCBEventStore\Types\EventTypes;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesAndTagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\TagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;
use Wwwision\DCBEventStore\Types\StreamQuery\StreamQuery;
use Wwwision\DCBEventStore\Types\Tags;

use function get_debug_type;
use function implode;
use function json_encode;
use function sprintf;
use function usleep;

use const JSON_THROW_ON_ERROR;

final class DoctrineEventStore implements EventStore, Setupable
{
    private readonly AbstractPlatform $platform;
    private readonly ClockInterface $clock;

    private int $dynamicParameterCount = 0;

    private function __construct(
        private readonly Connection $connection,
        private readonly string $eventTableName,
        ClockInterface $clock = null,
    ) {
        try {
            $this->platform = $this->connection->getDatabasePlatform();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to determine Database platform from connection: %s', $e->getMessage()), 1687001448, $e);
        }
        $this->clock = $clock ?? new SystemClock();
    }

    public static function create(Connection $connection, string $eventTableName): self
    {
        return new self($connection, $eventTableName);
    }

    public function setup(): void
    {
        try {
            $schemaManager = $this->connection->createSchemaManager();

            $schemaDiff = $schemaManager->createComparator()->compareSchemas($schemaManager->introspectSchema(), $this->databaseSchema());
            // TODO find replacement, @see https://github.com/doctrine/dbal/blob/3.6.x/UPGRADE.md#deprecated-schemadifftosql-and-schemadifftosavesql
            foreach ($schemaDiff->toSaveSql($this->platform) as $statement) {
                $this->connection->executeStatement($statement);
            }
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to setup event store: %s', $e->getMessage()), 1687010035, $e);
        }
    }

    /**
     * @throws SchemaException
     */
    private function databaseSchema(): Schema
    {
        $schema = new Schema();
        $table = $schema->createTable($this->eventTableName);
        // The monotonic sequence number
        $table->addColumn('sequence_number', Types::INTEGER, ['autoincrement' => true]);
        // The unique event id, usually a UUID
        $table->addColumn('id', Types::STRING, ['length' => 255]);
        // The event type in the format "<BoundedContext>:<EventType>"
        $table->addColumn('type', Types::STRING, ['length' => 255]);
        // The event payload (usually serialized as JSON)
        $table->addColumn('data', Types::TEXT);
        // The event domain identifiers as JSON
        $table->addColumn('tags', Types::JSON, ['platformOptions' => ['jsonb' => true]]);
        // When the event was appended originally
        $table->addColumn('recorded_at', Types::DATETIME_IMMUTABLE);

        $table->setPrimaryKey(['sequence_number']);
        $table->addUniqueIndex(['id']);
        return $schema;
    }

    public function read(StreamQuery $query, ?SequenceNumber $from = null): EventStream
    {
        $queryBuilder = $this->connection->createQueryBuilder()->select('*')->from($this->eventTableName)->orderBy('sequence_number', 'ASC');
        if ($from !== null) {
            $queryBuilder->andWhere('sequence_number >= :minimumSequenceNumber')->setParameter('minimumSequenceNumber', $from->value);
        }
        $this->addStreamQueryConstraints($queryBuilder, $query);
        return new DoctrineEventStream($queryBuilder->executeQuery());
    }

    public function readBackwards(StreamQuery $query, ?SequenceNumber $from = null): EventStream
    {
        $queryBuilder = $this->connection->createQueryBuilder()->select('*')->from($this->eventTableName)->orderBy('sequence_number', 'DESC');
        if ($from !== null) {
            $queryBuilder->andWhere('sequence_number <= :maximumSequenceNumber')->setParameter('maximumSequenceNumber', $from->value);
        }
        $this->addStreamQueryConstraints($queryBuilder, $query);
        return new DoctrineEventStream($queryBuilder->executeQuery());
    }

    public function append(Events $events, AppendCondition $condition): void
    {
        try {
            $this->reconnectDatabaseConnection();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to commit events because database connection could not be reconnected: %s', $e->getMessage()), 1685956292, $e);
        }
        Assert::eq($this->connection->getTransactionNestingLevel(), 0, 'Failed to commit events because a database transaction is active already');

        $parameters = [];
        $selects = [];
        $eventIndex = 0;
        foreach ($events as $event) {
            $selects[] = "SELECT :e{$eventIndex}_id id, :e{$eventIndex}_type type, :e{$eventIndex}_data data, :e{$eventIndex}_tags" . ($this->isPostgreSQL() ? '::jsonb' : '') . " tags, :e{$eventIndex}_recordedAt" . ($this->isPostgreSQL() ? '::timestamp' : '') . " recorded_at";
            try {
                $tags = json_encode($event->tags, JSON_THROW_ON_ERROR);
            } catch (JsonException $e) {
                throw new RuntimeException(sprintf('Failed to JSON encode tags: %s', $e->getMessage()), 1686304410, $e);
            }
            $parameters['e' . $eventIndex . '_id'] = $event->id->value;
            $parameters['e' . $eventIndex . '_type'] = $event->type->value;
            $parameters['e' . $eventIndex . '_data'] = $event->data->value;
            $parameters['e' . $eventIndex . '_tags'] = $tags;
            $parameters['e' . $eventIndex . '_recordedAt'] = $this->clock->now()->format('Y-m-d H:i:s');
            $eventIndex++;
        }
        $unionSelects = implode(' UNION ALL ', $selects);

        $statement = "INSERT INTO $this->eventTableName (id, type, data, tags, recorded_at) SELECT * FROM ( $unionSelects ) new_events";
        $queryBuilder = null;
        if (!$condition->expectedHighestSequenceNumber->isAny()) {
            $queryBuilder = $this->connection->createQueryBuilder()->select('sequence_number')->from($this->eventTableName)->orderBy('sequence_number', 'DESC')->setMaxResults(1);
            $this->addStreamQueryConstraints($queryBuilder, $condition->query);
            $parameters = [...$parameters, ...$queryBuilder->getParameters()];
            if ($condition->expectedHighestSequenceNumber->isNone()) {
                $subQuery = 'NOT EXISTS (' . $queryBuilder->getSQL() . ')';
            } else {
                $subQuery = '(' . $queryBuilder->getSQL() . ') = :highestSequenceNumber';
                $parameters['highestSequenceNumber'] = $condition->expectedHighestSequenceNumber->extractSequenceNumber()->value;
            }
            $statement .= " WHERE $subQuery";
        }
        $affectedRows = $this->commitStatement($statement, $parameters, $queryBuilder?->getParameterTypes() ?? []);
        if ($affectedRows === 0 && !$condition->expectedHighestSequenceNumber->isAny()) {
            throw $condition->expectedHighestSequenceNumber->isNone() ? ConditionalAppendFailed::becauseNoEventWhereExpected() : ConditionalAppendFailed::becauseNoEventMatchedTheQuery($condition->expectedHighestSequenceNumber);
        }
    }

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
                if ($this->isPostgreSQL()) {
                    $this->connection->beginTransaction();
                    $this->connection->executeStatement('LOCK TABLE ' . $this->eventTableName . ' IN SHARE UPDATE EXCLUSIVE MODE');
                }
                $affectedRows = (int)$this->connection->executeStatement($statement, $parameters, $parameterTypes);
                if ($this->isPostgreSQL()) {
                    $this->connection->commit();
                }
                return $affectedRows;
            } catch (DbalException $e) {
                if ($this->isPostgreSQL()) {
                    try {
                        $this->connection->rollBack();
                    } catch (Exception $e) {
                    }
                }
                if ((int)$e->getCode() !== 1213) {
                    throw new RuntimeException(sprintf('Failed to commit events: %s', $e->getMessage()), 1685956215, $e);
                }
                if ($retryAttempt >= $maxRetryAttempts) {
                    throw new RuntimeException(sprintf('Failed after %d retry attempts', $retryAttempt), 1686565685, $e);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
            }
        }
    }

    // -------------------------------------

    private function isPostgreSQL(): bool
    {
        return $this->platform instanceof PostgreSQLPlatform;
    }

    private function isSQLite(): bool
    {
        return $this->platform instanceof SqlitePlatform;
    }

    private function addStreamQueryConstraints(QueryBuilder $queryBuilder, StreamQuery $streamQuery): void
    {
        $this->dynamicParameterCount = 0;
        /** @var array<string> $criteriaConstraintStatements */
        $criteriaConstraintStatements = $streamQuery->criteria->map(fn (Criterion $criterion) => match ($criterion::class) {
            EventTypesCriterion::class => $this->eventTypeInStatement($queryBuilder, $criterion->eventTypes),
            TagsCriterion::class => $this->tagsIntersectStatement($queryBuilder, $criterion->tags),
            EventTypesAndTagsCriterion::class => (string)$queryBuilder->expr()->and($this->eventTypeInStatement($queryBuilder, $criterion->eventTypes), $this->tagsIntersectStatement($queryBuilder, $criterion->tags)),
            default => throw new RuntimeException(sprintf('Unsupported StreamQuery criterion "%s"', get_debug_type($criterion)), 1690909507),
        });
        if ($criteriaConstraintStatements === []) {
            return;
        }
        $queryBuilder->andWhere($queryBuilder->expr()->or(...$criteriaConstraintStatements));
    }

    private function tagsIntersectStatement(QueryBuilder $queryBuilder, Tags $tags): string
    {
        $tagsParameterName = $this->createUniqueParameterName();
        $queryBuilder->setParameter($tagsParameterName, json_encode($tags));
        if ($this->isSQLite()) {
            return "EXISTS(SELECT value FROM JSON_EACH($this->eventTableName.tags) WHERE value IN (SELECT value FROM JSON_EACH(:$tagsParameterName)))";
        }
        if ($this->isPostgreSQL()) {
            return "EXISTS(SELECT value FROM jsonb_array_elements_text($this->eventTableName.tags) WHERE value IN (SELECT value FROM jsonb_array_elements_text(:$tagsParameterName::jsonb)))";
        }
        return "JSON_OVERLAPS(tags, :$tagsParameterName)";
    }

    private function eventTypeInStatement(QueryBuilder $queryBuilder, EventTypes $eventTypes): string
    {
        $eventTypesParameterName = $this->createUniqueParameterName();
        $queryBuilder->setParameter($eventTypesParameterName, $eventTypes->toStringArray(), ArrayParameterType::STRING);
        return "type IN (:$eventTypesParameterName)";
    }

    private function createUniqueParameterName(): string
    {
        return 'param_' . (++$this->dynamicParameterCount);
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (Exception $_) {
            $this->connection->close();
            try {
                $this->connection->connect();
            } catch (DbalException $e) {
                throw new RuntimeException(sprintf('Failed to reconnect database connection: %s', $e->getMessage()), 1686045084, $e);
            }
        }
    }
}
