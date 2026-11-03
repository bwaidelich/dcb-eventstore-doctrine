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

use function hash;
use function sprintf;

final class DoctrineEventStoreConfiguration
{
    public readonly AbstractPlatform $platform;
    private int $dynamicParameterCount = 0;

    private function __construct(
        public readonly Connection $connection,
        public readonly string $eventTableName,
        public readonly ClockInterface $clock,
        public readonly bool $notificationsEnabled = false,
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
        return new self($this->connection, $this->eventTableName, $clock, $this->notificationsEnabled);
    }

    /**
     * Notifies waiters about appended events on PostgreSQL, without effect on other platforms
     *
     * This has to be enabled for all writers and workers. It slows down appends, even across event tables. See docs/decisions.md
     */
    public function withNotifications(): self
    {
        return new self($this->connection, $this->eventTableName, $this->clock, true);
    }

    /**
     * Name of the PostgreSQL channel: lower case and short enough for any table name. A collision only causes a spurious wake-up
     */
    public function notificationChannel(): string
    {
        return 'dcb_' . hash('crc32b', $this->eventTableName);
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
        return $this->platform instanceof PostgreSQLPlatform;
    }

    public function isSQLite(): bool
    {
        return $this->platform instanceof SQLitePlatform;
    }

    public function isMySQL(): bool
    {
        return $this->platform instanceof AbstractMySQLPlatform;
    }
}
