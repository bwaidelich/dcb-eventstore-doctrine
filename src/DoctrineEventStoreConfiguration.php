<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use DateTimeImmutable;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\AbstractMySQLPlatform;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Psr\Clock\ClockInterface;
use RuntimeException;

use function sprintf;

final class DoctrineEventStoreConfiguration
{
    /** @internal */
    public readonly AbstractPlatform $platform;
    private int $dynamicParameterCount = 0;

    private function __construct(
        /** @internal */
        public readonly Connection $connection,
        public readonly string $eventTableName,
        public readonly ClockInterface $clock,
    ) {
        try {
            $this->platform = $this->connection->getDatabasePlatform();
        } catch (DbalException $e) {
            throw new RuntimeException(sprintf('Failed to determine Database platform from connection: %s', $e->getMessage()), 1687001448, $e);
        }
    }

    public static function create(Connection $connection, string $eventTableName): self
    {
        return new self(
            $connection,
            $eventTableName,
            new class implements ClockInterface {
                public function now(): DateTimeImmutable
                {
                    return new DateTimeImmutable();
                }
            },
        );
    }

    public function withClock(ClockInterface $clock): self
    {
        return new self($this->connection, $this->eventTableName, $clock);
    }

    /** @internal */
    public function createUniqueParameterName(): string
    {
        return 'param_' . (++$this->dynamicParameterCount);
    }

    /** @internal */
    public function resetUniqueParameterCount(): void
    {
        $this->dynamicParameterCount = 0;
    }

    /** @internal */
    public function isPostgreSQL(): bool
    {
        return $this->platform instanceof PostgreSQLPlatform;
    }

    /** @internal */
    public function isSQLite(): bool
    {
        return $this->platform instanceof SQLitePlatform;
    }

    /** @internal */
    public function isMySQL(): bool
    {
        return $this->platform instanceof AbstractMySQLPlatform;
    }
}
