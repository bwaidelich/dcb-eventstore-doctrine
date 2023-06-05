<?php
declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Unit;

use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\Attributes\CoversClass;
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


}