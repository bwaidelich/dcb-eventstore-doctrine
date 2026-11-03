<?php

declare(strict_types=1);

/**
 * Appends a single event from a separate process, so that a test can observe an append blocking on another one.
 *
 * Usage: php append-event.php <dsn> <table> <data> <tag>
 */

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;

require __DIR__ . '/../../vendor/autoload.php';

Assert::count($argv, 5);
[, $dsn, $table, $data, $tag] = $argv;

$config = new Configuration();
$config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
$connection = DriverManager::getConnection((new DsnParser())->parse($dsn), $config);
DoctrineEventStore::create($connection, $table)->append(Event::create('SomeEvent', $data, [$tag], []));
