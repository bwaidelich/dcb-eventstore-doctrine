<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Closure;
use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaException;
use Doctrine\DBAL\Types\Types;
use JsonException;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Exception\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Model\Event;
use Wwwision\DCBEventStore\Model\EventId;
use Wwwision\DCBEventStore\Model\Events;
use Wwwision\DCBEventStore\Model\StreamQuery;
use RuntimeException;
use Webmozart\Assert\Assert;

use function assert;
use function json_encode;

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
        $this->reconnectDatabaseConnection();
        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->eventTableName)
            ->orderBy('sequence_number', 'ASC');

        if ($query->types !== null) {
            $queryBuilder->andWhere('type IN (:eventTypes)')->setParameter('eventTypes', $query->types->toStringArray(), ArrayParameterType::STRING);
        }
        if ($query->domainIds !== null) {
            $domainIdentifierConstraints = [];
            $domainIdParamIndex = 0;
            foreach ($query->domainIds as $key => $value) {
                $domainIdentifierConstraints[] = 'JSON_EXTRACT(domain_ids, \'$.' . $key . '\') = :domainIdParam' . $domainIdParamIndex;
                $queryBuilder->setParameter('domainIdParam' . $domainIdParamIndex, $value);
                $domainIdParamIndex++;
            }
            $queryBuilder->andWhere($queryBuilder->expr()->or(...$domainIdentifierConstraints));
        }
        return DoctrineEventStream::create($queryBuilder);
    }

    public function append(Events $events): void
    {
        $this->writeEvents($events);
    }

    public function conditionalAppend(Events $events, StreamQuery $query, ?EventId $lastEventId): void
    {
        $this->writeEvents($events, function () use ($query, $lastEventId) {
            $lastEvent = $this->stream($query)->last();
            if ($lastEvent === null) {
                if ($lastEventId !== null) {
                    throw ConditionalAppendFailed::becauseNoEventMatchedTheQuery();
                }
            } elseif ($lastEventId === null) {
                throw ConditionalAppendFailed::becauseNoEventWhereExpected();
            } elseif (!$lastEvent->event->id->equals($lastEventId)) {
                throw ConditionalAppendFailed::becauseEventIdsDontMatch($lastEventId, $lastEvent->event->id);
            }
        });
    }

    // -------------------------------------

    private function writeEvents(Events $events, Closure $condition = null): void
    {
        try {
            $this->reconnectDatabaseConnection();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to commit events because database connection could not be reconnected: %s', $e->getMessage()), 1685956292, $e);
        }
        Assert::eq($this->connection->getTransactionNestingLevel(), 0, 'Failed to commit events because a database transaction is active already');
        try {
            $this->connection->beginTransaction();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to commit events because a database transaction could not be started: %s', $e->getMessage()), 1685956072, $e);
        }
        if ($condition !== null) {
            $condition();
        }
        try {
            foreach ($events as $event) {
                $this->connection->insert($this->eventTableName, self::eventToDatabaseRow($event));
            }
            $lastInsertId = $this->connection->lastInsertId();
            Assert::numeric($lastInsertId, 'Failed to commit events because expected last insert id to be numeric, but it is: %s');
            $this->connection->commit();
        } catch (DbalException | JsonException $e) {
            try {
                $this->connection->rollBack();
            } catch (DbalException $e) {
            }
            throw new RuntimeException(sprintf('Failed to commit events: %s', $e->getMessage()), 1685956215, $e);
        }
    }

    /**
     * @return array{id: string, type: string, data: string, domain_ids: string}
     * @throws JsonException
     */
    private static function eventToDatabaseRow(Event $event): array
    {
        return [
            'id' => $event->id->value,
            'type' => $event->type->value,
            'data' => $event->data->value,
            'domain_ids' => json_encode($event->domainIds->jsonSerialize(), JSON_THROW_ON_ERROR),
        ];
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
