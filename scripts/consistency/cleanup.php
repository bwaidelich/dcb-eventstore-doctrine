<?php

require __DIR__ . '/../../vendor/autoload.php';
$caughtException = null;
try {
    \Wwwision\DCBEventStoreDoctrine\Tests\Integration\ConcurrencyTest::validateEvents();
} catch (Throwable $e) {
    $caughtException = $e;
}
\Wwwision\DCBEventStoreDoctrine\Tests\Integration\ConcurrencyTest::cleanup();
if ($caughtException !== null) {
    throw $caughtException;
}
