<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Wwwision\DCBEventStore\Event\Tag;
use Wwwision\DCBEventStoreDoctrine\TagPartition;

#[CoversClass(TagPartition::class)]
final class TagPartitionTest extends TestCase
{
    /**
     * The expected values freeze the hash contract. They were determined independently of this package:
     * `printf 'cart:1' | shasum -a 256` => 50ca6ce0... => 0x50ca6ce0 = 1355443424
     * Changing them means that all partitioned projections have to be rebuilt.
     *
     * @return iterable<array{string, string, int, int}>
     */
    public static function frozenHashContract(): iterable
    {
        yield ['cart:', 'cart:1', 8, 0];
        yield ['cart:', 'cart:1', 3, 2];
        yield ['cart:', 'cart:1', 1000, 424];
        yield ['cart:', 'cart:abc-123', 8, 3];
        yield ['cart:', 'cart:abc-123', 1000, 243];
        yield ['order_', 'order_item:Z', 8, 0];
        yield ['order_', 'order_item:Z', 1000, 952];
    }

    #[Test]
    #[DataProvider('frozenHashContract')]
    public function contains_returns_true_for_the_partition_of_the_tag(string $prefix, string $tag, int $partitions, int $expectedPartition): void
    {
        self::assertTrue(TagPartition::create($prefix, $partitions, $expectedPartition)->contains(Tag::fromString($tag)));
    }

    #[Test]
    #[DataProvider('frozenHashContract')]
    public function contains_returns_false_for_all_other_partitions(string $prefix, string $tag, int $partitions, int $expectedPartition): void
    {
        $otherPartition = ($expectedPartition + 1) % $partitions;
        self::assertFalse(TagPartition::create($prefix, $partitions, $otherPartition)->contains(Tag::fromString($tag)));
    }

    #[Test]
    public function contains_returns_false_for_tags_without_the_prefix(): void
    {
        $partition = TagPartition::create('cart:', 1, 0);

        self::assertTrue($partition->contains(Tag::fromString('cart:1')));
        self::assertFalse($partition->contains(Tag::fromString('product:1')));
        self::assertFalse($partition->contains(Tag::fromString('Cart:1')));
        self::assertFalse($partition->contains(Tag::fromString('xcart:1')));
    }

    #[Test]
    public function prefix_does_not_have_to_end_with_a_colon(): void
    {
        self::assertTrue(TagPartition::create('cart', 1, 0)->contains(Tag::fromString('cartoon:1')));
    }

    /**
     * @return iterable<array{string, int, int}>
     */
    public static function invalidArguments(): iterable
    {
        yield 'empty prefix' => ['', 1, 0];
        yield 'prefix with invalid characters' => ['cart%', 1, 0];
        yield 'prefix longer than a tag' => [str_repeat('a', 151), 1, 0];
        yield 'no partitions' => ['cart:', 0, 0];
        yield 'negative partitions' => ['cart:', -1, 0];
        yield 'more partitions than hash values' => ['cart:', 4294967297, 0];
        yield 'negative partition' => ['cart:', 2, -1];
        yield 'partition equal to partitions' => ['cart:', 2, 2];
        yield 'partition above partitions' => ['cart:', 2, 3];
    }

    #[Test]
    #[DataProvider('invalidArguments')]
    public function create_rejects_invalid_arguments(string $prefix, int $partitions, int $partition): void
    {
        $this->expectException(InvalidArgumentException::class);
        TagPartition::create($prefix, $partitions, $partition);
    }

    #[Test]
    public function create_accepts_the_highest_possible_number_of_partitions(): void
    {
        $partition = TagPartition::create('cart:', 4294967296, 1355443424);

        self::assertTrue($partition->contains(Tag::fromString('cart:1')));
    }
}
