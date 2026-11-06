<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Exception as DbalException;
use InvalidArgumentException;
use PDO;
use Pdo\Pgsql;
use RuntimeException;

use function ceil;
use function sprintf;

/**
 * Blocks on a LISTEN connection and compares the head if a wait ends without a notification. See docs/decisions.md
 */
final class PostgresNotificationWaiter implements EventStoreWaiter
{
    private bool $listening = false;
    private int|null $lastHead = null;

    public function __construct(
        private readonly DoctrineEventStoreConfiguration $config,
    ) {}

    public function waitForNewEvents(float $timeoutSeconds): bool
    {
        if ($timeoutSeconds < 0.0) {
            throw new InvalidArgumentException(sprintf('The timeout must not be negative, got: %f', $timeoutSeconds), 1793750011);
        }
        if ($this->config->connection->isTransactionActive()) {
            throw new RuntimeException('The connection of a waiter must not be inside a transaction. Is it used for something else?', 1793750012);
        }
        if (!$this->listening) {
            $this->listen();
            return true;
        }
        try {
            if ($this->receiveNotification((int) ceil($timeoutSeconds * 1000))) {
                // All queued notifications result in a single wake-up
                do {
                    $received = $this->receiveNotification(0);
                } while ($received);
                $this->headHasChanged();
                return true;
            }
            return $this->headHasChanged();
        } catch (DbalException|RuntimeException) {
            // PDO raises a generic error for a lost connection, so it can't be told apart from other errors. Those will fail again in listen()
            $this->listen();
            return true;
        }
    }

    /**
     * @throws DbalException | RuntimeException
     */
    private function receiveNotification(int $timeoutMilliseconds): bool
    {
        $pdo = $this->pdo();
        // getNotify() doesn't exist on PHP 8.3 or on a plain PDO instance, pgsqlGetNotify() is deprecated since PHP 8.5
        $method = $pdo instanceof Pgsql ? 'getNotify' : 'pgsqlGetNotify';
        return $pdo->{$method}(PDO::FETCH_ASSOC, $timeoutMilliseconds) !== false;
    }

    /**
     * @throws DbalException
     */
    private function pdo(): PDO
    {
        $connection = $this->config->connection->getNativeConnection();
        if (!$connection instanceof PDO || $connection->getAttribute(PDO::ATTR_DRIVER_NAME) !== 'pgsql') {
            throw new RuntimeException('Waiting for notifications requires a connection that uses the pdo_pgsql driver', 1793750013);
        }
        return $connection;
    }

    /**
     * @throws RuntimeException
     */
    private function headHasChanged(): bool
    {
        $head = (new DoctrineEventStore($this->config))->head()?->value;
        $changed = $head !== $this->lastHead;
        $this->lastHead = $head;
        return $changed;
    }

    private function listen(): void
    {
        $this->listening = false;
        try {
            $this->config->connection->close();
            $this->pdo();
            $this->config->connection->executeStatement('LISTEN ' . $this->config->notificationChannel());
            // Has to happen after LISTEN, so that nothing can slip through between the two
            $this->headHasChanged();
            $this->listening = true;
        } catch (DbalException|RuntimeException $e) {
            $this->config->connection->close();
            throw new RuntimeException(sprintf('Failed to listen for notifications: %s', $e->getMessage()), 1793750014, $e);
        }
    }
}
