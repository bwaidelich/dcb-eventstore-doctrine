<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Exception as DbalException;
use InvalidArgumentException;
use RuntimeException;

use function hrtime;
use function intdiv;
use function is_array;
use function min;
use function mt_getrandmax;
use function mt_rand;
use function sprintf;
use function time_nanosleep;

/**
 * Checks the head of the event store periodically. See docs/decisions.md
 */
final class PollingWaiter implements EventStoreWaiter
{
    private const JITTER = 0.2;

    private bool $initialized = false;
    private int|null $lastHead = null;

    public function __construct(
        private readonly DoctrineEventStoreConfiguration $config,
        private readonly float $pollIntervalSeconds = 1.0,
    ) {
        if ($this->pollIntervalSeconds <= 0.0) {
            throw new InvalidArgumentException(sprintf('The poll interval must be greater than 0, got: %f', $this->pollIntervalSeconds), 1793750001);
        }
    }

    public function waitForNewEvents(float $timeoutSeconds): bool
    {
        if ($timeoutSeconds < 0.0) {
            throw new InvalidArgumentException(sprintf('The timeout must not be negative, got: %f', $timeoutSeconds), 1793750002);
        }
        if ($this->config->connection->isTransactionActive()) {
            throw new RuntimeException('The connection of a waiter must not be inside a transaction. Is it used for something else?', 1793750003);
        }
        if (!$this->initialized) {
            $this->initialize();
            return true;
        }
        try {
            return $this->pollUntil(hrtime(true) + (int) ($timeoutSeconds * 1E9));
        } catch (DbalException) {
            $this->config->connection->close();
            $this->initialize();
            return true;
        }
    }

    /**
     * @throws DbalException
     */
    private function pollUntil(int $deadlineNanoseconds): bool
    {
        while (true) {
            if ($this->headHasChanged()) {
                return true;
            }
            $remainingNanoseconds = $deadlineNanoseconds - (int) hrtime(true);
            if ($remainingNanoseconds <= 0) {
                return false;
            }
            $jitter = (1 - self::JITTER) + (2 * self::JITTER * (mt_rand() / mt_getrandmax()));
            $sleepNanoseconds = min((int) ($this->pollIntervalSeconds * $jitter * 1E9), $remainingNanoseconds);
            if (is_array(time_nanosleep(intdiv($sleepNanoseconds, 1_000_000_000), $sleepNanoseconds % 1_000_000_000))) {
                // Interrupted by a signal: don't resume, so that the worker can react to it
                return $this->headHasChanged();
            }
        }
    }

    /**
     * @throws DbalException
     */
    private function headHasChanged(): bool
    {
        $head = (new DoctrineEventStore($this->config))->head()?->value;
        $changed = $head !== $this->lastHead;
        $this->lastHead = $head;
        return $changed;
    }

    private function initialize(): void
    {
        try {
            $this->headHasChanged();
            $this->initialized = true;
        } catch (DbalException $e) {
            $this->config->connection->close();
            $this->initialized = false;
            throw new RuntimeException(sprintf('Failed to connect waiter: %s', $e->getMessage()), 1793750004, $e);
        }
    }
}
