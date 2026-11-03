<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use InvalidArgumentException;
use RuntimeException;

/**
 * Lets an idle worker wait for new events instead of polling on its own. See {@see DoctrineEventStore::createWaiter()}
 */
interface EventStoreWaiter
{
    /**
     * Blocks until there might be new events, or until the timeout expires
     *
     * The first call returns immediately, so that a worker can catch up after the waiter became active.
     *
     * @param float $timeoutSeconds Maximum time to wait, 0.0 to not wait at all. A signal can end the wait early
     * @return bool TRUE if there might be new events, which can be spurious. FALSE if the head didn't change since the last check
     * @throws InvalidArgumentException if the timeout is negative
     * @throws RuntimeException if the database can't be reached. The next call tries to reconnect
     */
    public function waitForNewEvents(float $timeoutSeconds): bool;
}
