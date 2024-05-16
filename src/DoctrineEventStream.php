<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use DateTimeImmutable;
use Doctrine\DBAL\Result;
use Traversable;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Types\Event;
use Wwwision\DCBEventStore\Types\EventData;
use Wwwision\DCBEventStore\Types\EventEnvelope;
use Wwwision\DCBEventStore\Types\EventId;
use Wwwision\DCBEventStore\Types\EventMetadata;
use Wwwision\DCBEventStore\Types\EventType;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBEventStore\Types\StreamQuery\CriterionHashes;
use Wwwision\DCBEventStore\Types\Tags;

final class DoctrineEventStream implements EventStream
{
    public function __construct(private readonly Result $result)
    {
    }

    public function getIterator(): Traversable
    {
        while (($row = $this->result->fetchAssociative()) !== false) {
            yield self::databaseRowToEventEnvelope($row);
        }
    }

    public function first(): ?EventEnvelope
    {
        $row = $this->result->fetchAssociative();
        if ($row === false) {
            return null;
        }
        return self::databaseRowToEventEnvelope($row);
    }

    // -----------------------------------

    /**
     * @param array<mixed> $row
     * @return EventEnvelope
     */
    private static function databaseRowToEventEnvelope(array $row): EventEnvelope
    {
        Assert::numeric($row['sequence_number']);
        $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['recorded_at']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new EventEnvelope(
            SequenceNumber::fromInteger((int)$row['sequence_number']),
            $recordedAt,
            $row['criterion_hashes'] === '' ? CriterionHashes::none() : CriterionHashes::fromArray(explode(',', $row['criterion_hashes'])),
            new Event(
                EventId::fromString($row['id']),
                EventType::fromString($row['type']),
                EventData::fromString($row['data']),
                Tags::fromJson($row['tags']),
                $row['metadata'] === null ? EventMetadata::none() : EventMetadata::fromJson($row['metadata']),
            ),
        );
    }
}
