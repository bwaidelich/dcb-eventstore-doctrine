<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use DateTimeImmutable;
use Doctrine\DBAL\Result;
use Traversable;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\SequencePosition;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStore\SequencedEvents;

final class DoctrineSequencedEvents implements SequencedEvents
{
    public function __construct(private readonly Result $result) {}

    public function getIterator(): Traversable
    {
        while (($row = $this->result->fetchAssociative()) !== false) {
            yield self::databaseRowToSequencedEvent($row);
        }
    }

    public function first(): SequencedEvent|null
    {
        $row = $this->result->fetchAssociative();
        if ($row === false) {
            return null;
        }
        return self::databaseRowToSequencedEvent($row);
    }

    // -----------------------------------

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
        Assert::string($row['metadata']);
        $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['recorded_at']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new SequencedEvent(
            SequencePosition::fromInteger((int) $row['sequence_number']),
            $recordedAt,
            Event::create(
                $row['type'],
                $row['data'],
                $row['tags'],
                $row['metadata'],
            ),
        );
    }
}
