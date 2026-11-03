<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\Event\Tag;

use function hash;
use function hexdec;
use function str_starts_with;
use function substr;

/**
 * Selects the events with a tag that starts with a given prefix and hashes into one of a number of partitions
 *
 * A tag is assigned to the partition "first 32 bits of the SHA-256 of the tag as unsigned integer, modulo number of partitions".
 * This must not change, because it would move tags between the partitions of existing projections.
 * {@see DoctrineEventStore::partitionConstraint()} has to implement the same calculation in SQL.
 *
 * @see DoctrineEventStore::readPartition()
 */
final class TagPartition
{
    private const int PARTITIONS_MAX = 2 ** 32;

    private function __construct(
        public readonly string $prefix,
        public readonly int $partitions,
        public readonly int $partition,
    ) {}

    /**
     * @param string $prefix Only tags starting with this string are considered (literal and case-sensitive). A trailing colon is not enforced: "cart" also matches "cartoon:1"
     * @param int $partitions The total number of partitions
     * @param int $partition The zero-based number of this partition
     */
    public static function create(string $prefix, int $partitions, int $partition): self
    {
        Assert::regex($prefix, '/^[[:alnum:]\-\_\:]{1,' . Tag::LENGTH_MAX . '}$/', 'The prefix must only consist of characters that are valid in tags and must be between 1 and ' . Tag::LENGTH_MAX . ' characters long, given: %s');
        Assert::range($partitions, 1, self::PARTITIONS_MAX, 'The number of partitions must be between %2$d and %3$d, given: %1$d');
        Assert::range($partition, 0, $partitions - 1, 'The partition must be between %2$d and %3$d, given: %1$d');
        return new self($prefix, $partitions, $partition);
    }

    /**
     * Whether this partition owns the given tag
     *
     * An event with multiple tags can be returned by multiple partitions, so this can be used to determine which of its tags to handle
     */
    public function contains(Tag $tag): bool
    {
        return str_starts_with($tag->value, $this->prefix)
            && ((int) hexdec(substr(hash('sha256', $tag->value), 0, 8))) % $this->partitions === $this->partition;
    }
}
