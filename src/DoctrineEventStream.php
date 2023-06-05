<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Model\DomainIds;
use Wwwision\DCBEventStore\Model\Event;
use Wwwision\DCBEventStore\Model\EventData;
use Wwwision\DCBEventStore\Model\EventEnvelope;
use Wwwision\DCBEventStore\Model\EventId;
use Wwwision\DCBEventStore\Model\EventType;
use Wwwision\DCBEventStore\Model\SequenceNumber;
use Traversable;
use function is_numeric;

final readonly class DoctrineEventStream implements EventStream
{
    private function __construct(
        private QueryBuilder $queryBuilder,
        private ?SequenceNumber $minimumSequenceNumber,
        private ?int $limit,
    ) {}

    public static function create(QueryBuilder $queryBuilder): self
    {
        return new self($queryBuilder, null, null);
    }

    public function withMinimumSequenceNumber(SequenceNumber $sequenceNumber): EventStream
    {
        if ($this->minimumSequenceNumber !== null && $sequenceNumber->equals($this->minimumSequenceNumber)) {
            return $this;
        }
        return new self($this->queryBuilder, $sequenceNumber, $this->limit);
    }

    public function limit(int $limit): self
    {
        if ($limit === $this->limit) {
            return $this;
        }
        return new self($this->queryBuilder, $this->minimumSequenceNumber, $limit);
    }

    public function last(): ?EventEnvelope
    {
        $queryBuilder = clone $this->queryBuilder;
        $queryBuilder = $queryBuilder
            ->orderBy('sequence_number', 'DESC')
            ->setMaxResults(1);
        $row = $queryBuilder->fetchAssociative();
        if ($row === false) {
            return null;
        }
        return self::databaseRowToEventEnvelope($row);
    }

    public function getIterator(): Traversable
    {
        $this->reconnectDatabaseConnection();

        /** @var Result $result */
        $result = $this->queryBuilder->executeQuery();
        /** @var array<string, string> $row */
        foreach ($result->fetchAllAssociative() as $row) {
            yield self::databaseRowToEventEnvelope($row);
        }
    }

    // -----------------------------------

    /**
     * @param array<mixed> $row
     * @return EventEnvelope
     */
    private static function databaseRowToEventEnvelope(array $row): EventEnvelope
    {
        assert(is_numeric($row['sequence_number']));
        return new EventEnvelope(
            SequenceNumber::fromInteger((int)$row['sequence_number']),
            new Event(
                EventId::fromString($row['id'] ?? ''),
                EventType::fromString($row['type'] ?? ''),
                EventData::fromString($row['data'] ?? ''),
                DomainIds::fromJson($row['domain_ids'] ?? ''),
            ),
        );
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->queryBuilder->getConnection()->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->queryBuilder->getConnection()->close();
            $this->queryBuilder->getConnection()->connect();
        }
    }
}