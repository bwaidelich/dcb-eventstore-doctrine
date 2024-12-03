<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use PHPUnit\Framework\Attributes\CoversNothing;
use Wwwision\DCBEventStore\EventStore;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreConcurrencyTestBase;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use function getenv;
use function is_string;
use const PHP_EOL;

#[CoversNothing]
final class ConcurrencyTest extends EventStoreConcurrencyTestBase
{

    private static ?DoctrineEventStore $eventStore = null;
    private static ?Connection $connection = null;

    public static function prepare(): void
    {
        $connection = self::connection();
        $eventStore = self::createEventStore();
        $eventStore->setup();
        if ($connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $connection->executeStatement('TRUNCATE TABLE ' . self::eventTableName() . ' RESTART IDENTITY');
        } elseif ($connection->getDatabasePlatform() instanceof SqlitePlatform) {
            /** @noinspection SqlWithoutWhere */
            $connection->executeStatement('DELETE FROM ' . self::eventTableName());
            $connection->executeStatement('DELETE FROM sqlite_sequence WHERE name =\'' . self::eventTableName() . '\'');
        } else {
            $connection->executeStatement('TRUNCATE TABLE ' . self::eventTableName());
        }
        echo PHP_EOL . 'Prepared tables for ' . $connection->getDatabasePlatform()::class . PHP_EOL;
    }

    public static function cleanup(): void
    {
        self::connection()->executeStatement(self::connection()->getDatabasePlatform()->getTruncateTableSQL(self::eventTableName(), true));
    }

    protected static function createEventStore(): EventStore
    {
        if (self::$eventStore === null) {
            self::$eventStore = DoctrineEventStore::create(self::connection(), self::eventTableName());
        }
        return self::$eventStore;
    }

    private static function connection(): Connection
    {
        if (self::$connection === null) {
            $dsn = getenv('DCB_TEST_DSN');
            if (!is_string($dsn)) {
                $dsn = 'sqlite:///events_test.sqlite';
            }
            self::$connection = DriverManager::getConnection(['url' => $dsn]);
        }
        return self::$connection;
    }

    private static function eventTableName(): string
    {
        return 'dcb_events_test';
    }

}