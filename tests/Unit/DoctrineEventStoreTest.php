<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Unit;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\AssertionFailedError;
use PHPUnit\Framework\Attributes\CoversClass;
use Wwwision\DCBEventStore\Exception\ConditionalAppendFailed;
use Wwwision\DCBEventStore\Model\DomainIds;
use Wwwision\DCBEventStore\Model\EventData;
use Wwwision\DCBEventStore\Model\EventId;
use Wwwision\DCBEventStore\Model\Events;
use Wwwision\DCBEventStore\Model\EventType;
use Wwwision\DCBEventStore\Model\StreamQuery;
use Wwwision\DCBEventStore\Tests\Unit\EventStoreTestBase;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

#[CoversClass(DoctrineEventStore::class)]
final class DoctrineEventStoreTest extends EventStoreTestBase
{
    protected function createEventStore(): DoctrineEventStore
    {
        $connection = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
        return DoctrineEventStore::create($connection, 'some_table_name');
    }


    /**
     * This test tries to expose a race condition by intercepting the INSERT operation
     * And committing new events in the meantime.
     *
     * NOTE: This test does not guarantee, that all race conditions are caught because it does not cover multiple processes interacting with the same backend
     *
     * @see https://github.com/bwaidelich/dcb-eventstore-doctrine/issues/2
     * @see https://github.com/bwaidelich/dcb-eventstore/issues/3
     */
    public function test_race_condition(): void
    {
        $connection = DriverManager::getConnection(['url' => 'sqlite:///:memory:']);
        $interceptedConnection = new class ($connection) extends Connection {
            public function __construct(private Connection $originalConnection) {
                parent::__construct($this->originalConnection->getParams(), $this->originalConnection->getDriver(), $this->originalConnection->getConfiguration());
            }

            public function executeStatement($sql, array $params = [], array $types = []) {
                $query = StreamQuery::matchingIds(DomainIds::single('foo', 'bar'));
                $chaosMonkeyEventStore = DoctrineEventStore::create($this->originalConnection, 'some_table_name');
                $chaosMonkeyEventStore->setup();
                try {
                    $chaosMonkeyEventStore->append(Events::single(EventId::fromString('e2'), EventType::fromString('SomeEventType'), EventData::fromString('ignore'), DomainIds::single('foo', 'bar')), $query, null);
                } catch (\Throwable $e) {
                    throw new AssertionFailedError('Failed', 1686303622, $e);
                }
                return $this->originalConnection->executeStatement($sql, $params, $types);
            }
        };
        $eventStore1 = DoctrineEventStore::create($interceptedConnection, 'some_table_name');

        $query = StreamQuery::matchingIds(DomainIds::single('foo', 'bar'));

        $this->expectException(ConditionalAppendFailed::class);
        $eventStore1->append(Events::single(EventId::fromString('e1'), EventType::fromString('SomeEventType'), EventData::fromString('ignore'), DomainIds::single('foo', 'bar')), $query, null);
    }

}