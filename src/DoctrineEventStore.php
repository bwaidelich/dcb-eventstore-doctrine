<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaException;
use Doctrine\DBAL\Types\Types;
use JsonException;
use RuntimeException;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Exception\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Helper\InMemoryEventStream;
use Wwwision\DCBEventStore\Model\EventId;
use Wwwision\DCBEventStore\Model\Events;
use Wwwision\DCBEventStore\Model\StreamQuery;

use function assert;
use function get_debug_type;
use function implode;
use function json_encode;
use function sprintf;

use const JSON_FORCE_OBJECT;
use const JSON_THROW_ON_ERROR;

final readonly class DoctrineEventStore implements EventStore
{
    private function __construct(
        private Connection $connection,
        private string $eventTableName,
    ) {
    }

    public static function create(Connection $connection, string $eventTableName): self
    {
        return new self($connection, $eventTableName);
    }

    /**
     * @throws DbalException
     */
    public function setup(): void
    {
        $platform = $this->connection->getDatabasePlatform();
        assert($platform instanceof AbstractPlatform);
        $schemaManager = $this->connection->createSchemaManager();

        $schemaDiff = $schemaManager->createComparator()->compareSchemas($schemaManager->introspectSchema(), $this->databaseSchema());
        // TODO find replacement, @see https://github.com/doctrine/dbal/blob/3.6.x/UPGRADE.md#deprecated-schemadifftosql-and-schemadifftosavesql
        foreach ($schemaDiff->toSaveSql($platform) as $statement) {
            $this->connection->executeStatement($statement);
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
        $table->addColumn('domain_ids', Types::TEXT);

        $table->setPrimaryKey(['sequence_number']);
        $table->addUniqueIndex(['id'], 'id_uniq');
        return $schema;
    }

    public function stream(StreamQuery $query): EventStream
    {
        if ($query->matchesNone()) {
            return InMemoryEventStream::empty();
        }
        try {
            $this->reconnectDatabaseConnection();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to reconnect database connection: %s', $e->getMessage()), 1686045084, $e);
        }
        $queryBuilder = $this->connection->createQueryBuilder()->select('*')->from($this->eventTableName)->orderBy('sequence_number', 'ASC');
        self::addStreamQueryConstraints($queryBuilder, $query);
        return DoctrineEventStream::create($queryBuilder);
    }

    public function append(Events $events, StreamQuery $query, ?EventId $lastEventId): void
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
            $selects[] = "SELECT :e{$eventIndex}_id, :e{$eventIndex}_type, :e{$eventIndex}_data, :e{$eventIndex}_domainIds";
            try {
                $domainIds = json_encode($event->domainIds, JSON_THROW_ON_ERROR | JSON_FORCE_OBJECT);
            } catch (JsonException $e) {
                throw new RuntimeException(sprintf('Failed to JSON encode domain ids: %s', $e->getMessage()), 1686304410, $e);
            }
            $parameters['e' . $eventIndex . '_id'] = $event->id->value;
            $parameters['e' . $eventIndex . '_type'] = $event->type->value;
            $parameters['e' . $eventIndex . '_data'] = $event->data->value;
            $parameters['e' . $eventIndex . '_domainIds'] = $domainIds;
            $eventIndex++;
        }
        $unionSelects = implode(' UNION ALL ', $selects);

        $queryBuilder = $this->connection->createQueryBuilder()->select('id')->from($this->eventTableName)->orderBy('sequence_number', 'DESC')->setMaxResults(1);
        self::addStreamQueryConstraints($queryBuilder, $query);
        $parameters = [...$parameters, ...$queryBuilder->getParameters()];
        if ($lastEventId === null) {
            $subQuery = 'NOT EXISTS (' . $queryBuilder->getSQL() . ')';
        } else {
            $subQuery = '(' . $queryBuilder->getSQL() . ') = :lastEventId';
            $parameters['lastEventId'] = $lastEventId->value;
        }

        $statement =
            <<<STATEMENT
                INSERT INTO $this->eventTableName (id, type, data, domain_ids)
                SELECT * FROM ( $unionSelects ) new_events
                WHERE $subQuery
            STATEMENT;
        try {
            $affectedRows = $this->connection->executeStatement($statement, $parameters);
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to commit events: %s', $e->getMessage()), 1685956215, $e);
        }
        Assert::numeric($affectedRows, sprintf('Expected INSERT statement to return number of affected rows, but got a result of type %s', get_debug_type($affectedRows)));
        if ((int)$affectedRows === 0) {
            throw $lastEventId === null ? ConditionalAppendFailed::becauseNoEventWhereExpected() : ConditionalAppendFailed::becauseNoEventMatchedTheQuery();
        }
    }

    // -------------------------------------

    private static function addStreamQueryConstraints(QueryBuilder $queryBuilder, StreamQuery $streamQuery): void
    {
        if ($streamQuery->matchesNone()) {
            $queryBuilder->andWhere('0');
            return;
        }
        if ($streamQuery->types !== null) {
            $queryBuilder->andWhere('type IN (:eventTypes)')
                // NOTE: For some reason, the following does not seem to work: ->setParameter('eventTypes', $query->types->toStringArray(), ArrayParameterType::STRING)
                ->setParameter('eventTypes', implode(',', $streamQuery->types->toStringArray()));
        }
        if ($streamQuery->domainIds !== null) {
            $domainIdentifierConstraints = [];
            $domainIdParamIndex = 0;
            foreach ($streamQuery->domainIds as $key => $value) {
                $domainIdentifierConstraints[] = 'JSON_EXTRACT(domain_ids, \'$.' . $key . '\') = :domainIdParam' . $domainIdParamIndex;
                $queryBuilder->setParameter('domainIdParam' . $domainIdParamIndex, $value);
                $domainIdParamIndex++;
            }
            if ($domainIdentifierConstraints !== []) {
                $queryBuilder->andWhere($queryBuilder->expr()->or(...$domainIdentifierConstraints));
            }
        }
    }

    /**
     * @throws DbalException
     */
    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
