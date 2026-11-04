# Upgrading

## Upgrading to 7.0

### PostgreSQL

Previous versions did not serialize appends on PostgreSQL:

- **Deployment:** stop all processes running a previous version before the new version starts appending. While both run, subscribers can miss events and append conditions can be violated.
- **Past events:** subscribers might have skipped events already, and this can't be detected or repaired automatically. If that is a concern, rebuild their projections.
- **Throughput:** appends no longer run in parallel, see [Event ordering](advanced.md#event-ordering).
- **Lock timeout:** an append that doesn't get the lock within 30 seconds fails with a `RuntimeException`.
- **Other writers:** anything else that writes to the event table has to take the same lock, see [Event ordering](advanced.md#event-ordering).

### All platforms

- **Exceptions:** `read()`, `readPartition()`, `head()` and the iteration over read events throw a `RuntimeException` instead of DBAL exceptions. Code that catches a specific DBAL exception has to inspect `getPrevious()` instead, see [Exceptions](advanced.md#exceptions).
- **Internal API:** `connection`, `platform`, `notificationChannel()`, `createUniqueParameterName()`, `resetUniqueParameterCount()`, `isPostgreSQL()`, `isSQLite()` and `isMySQL()` of `DoctrineEventStoreConfiguration` are now `@internal`. They still work, but can change without a major version.
- **Index names:** index names now contain a hash of the table name. Existing tables work without a change. Running `setup()` once renames their indexes, and the GIN index on PostgreSQL isn't rebuilt.
- **Dependencies:** `ext-pdo` is now required explicitly.
