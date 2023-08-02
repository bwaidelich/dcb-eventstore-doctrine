<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Result;
use Traversable;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Types\Event;
use Wwwision\DCBEventStore\Types\EventData;
use Wwwision\DCBEventStore\Types\EventEnvelope;
use Wwwision\DCBEventStore\Types\EventId;
use Wwwision\DCBEventStore\Types\EventType;
use Wwwision\DCBEventStore\Types\SequenceNumber;
use Wwwision\DCBEventStore\Types\Tags;

use function assert;
use function is_numeric;

final readonly class DoctrineEventStream implements EventStream
{
    public function __construct(private Result $result)
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
        assert(is_numeric($row['sequence_number']));
        return new EventEnvelope(
            SequenceNumber::fromInteger((int)$row['sequence_number']),
            new Event(
                EventId::fromString($row['id'] ?? ''),
                EventType::fromString($row['type'] ?? ''),
                EventData::fromString($row['data'] ?? ''),
                Tags::fromJson($row['tags'] ?? ''),
            ),
        );
    }
}
