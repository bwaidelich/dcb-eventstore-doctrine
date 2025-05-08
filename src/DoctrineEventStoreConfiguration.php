<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Psr\Clock\ClockInterface;
use RuntimeException;
use Wwwision\DCBEventStore\Helpers\SystemClock;

final class DoctrineEventStoreConfiguration
{
    public readonly AbstractPlatform $platform;
    private int $dynamicParameterCount = 0;

    public function __construct(
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
            new SystemClock(),
        );
    }

    public function withClock(ClockInterface $clock): self
    {
        return new self($this->connection, $this->eventTableName, $clock);
    }

    public function createUniqueParameterName(): string
    {
        return 'param_' . (++$this->dynamicParameterCount);
    }

    public function resetUniqueParameterCount(): void
    {
        $this->dynamicParameterCount = 0;
    }

    public function isPostgreSQL(): bool
    {
        return $this->platform instanceof PostgreSqlPlatform;
    }

    public function isSQLite(): bool
    {
        return $this->platform instanceof SqlitePlatform;
    }
}
