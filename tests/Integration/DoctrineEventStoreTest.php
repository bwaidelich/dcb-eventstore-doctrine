<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use PHPUnit\Framework\Attributes\CoversClass;
use Wwwision\DCBEventStore\Tests\Integration\EventStoreTestBase;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use function getenv;
use function is_string;

#[CoversClass(DoctrineEventStore::class)]
final class DoctrineEventStoreTest extends EventStoreTestBase
{
    protected function createEventStore(): DoctrineEventStore
    {
        $eventTableName = 'dcb_events_test';

        $dsn = getenv('DCB_TEST_DSN');
        if (!is_string($dsn)) {
            $dsn = 'sqlite:///events_test.sqlite';
        }
        $connection = DriverManager::getConnection(['url' => $dsn]);
        $eventStore = DoctrineEventStore::create($connection, $eventTableName);
        $eventStore->setup();

        if ($connection->getDatabasePlatform() instanceof SqlitePlatform) {
            $connection->executeStatement('DELETE FROM ' . $eventTableName);
            $connection->executeStatement('UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME="' . $eventTableName . '"');
        } elseif ($connection->getDatabasePlatform() instanceof PostgreSQLPlatform) {
            $connection->executeStatement('TRUNCATE TABLE ' . $eventTableName . ' RESTART IDENTITY');
        } else {
            $connection->executeStatement('TRUNCATE TABLE ' . $eventTableName);
        }
        return $eventStore;
    }

}