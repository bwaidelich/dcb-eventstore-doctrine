<?php

declare(strict_types=1);

/**
 * Appends a single event from a separate process, so that a test can observe an append blocking on another one.
 *
 * Usage: php append-event.php <dsn> <table> <data> <tag> [delay in seconds]
 */

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;

require __DIR__ . '/../../vendor/autoload.php';

$arguments = $_SERVER['argv'] ?? null;
Assert::isList($arguments);
Assert::allString($arguments);
Assert::inArray(count($arguments), [5, 6]);
[, $dsn, $table, $data, $tag] = $arguments;
usleep((int) (((float) ($arguments[5] ?? 0)) * 1E6));

$config = new Configuration();
$config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
$connection = DriverManager::getConnection((new DsnParser())->parse($dsn), $config);
(new DoctrineEventStore(DoctrineEventStoreConfiguration::create($connection, $table)))->append(Event::create('SomeEvent', $data, [$tag], []));
