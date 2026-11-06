<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\Tests\Integration;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Doctrine\DBAL\Schema\DefaultSchemaManagerFactory;
use Doctrine\DBAL\Tools\DsnParser;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Throwable;
use Wwwision\DCBEventStore\Event\Event;
use Wwwision\DCBEventStore\Event\Events;
use Wwwision\DCBEventStore\Event\Tag;
use Wwwision\DCBEventStore\Query\Query;
use Wwwision\DCBEventStore\SequencedEvent\SequencedEvent;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStore;
use Wwwision\DCBEventStoreDoctrine\TagPartition;

use function array_map;
use function escapeshellarg;
use function exec;
use function extract;
use function file_get_contents;
use function file_put_contents;
use function function_exists;
use function get_defined_vars;
use function getenv;
use function implode;
use function is_string;
use function ob_end_clean;
use function ob_get_clean;
use function ob_start;
use function preg_match_all;
use function preg_replace;
use function str_replace;
use function sys_get_temp_dir;
use function tempnam;
use function unlink;

use const E_ALL;
use const E_DEPRECATED;
use const PHP_BINARY;
use const PREG_SET_ORDER;

/**
 * Executes the PHP blocks of the documentation in order and compares their output to a directly following "text" block
 *
 * All connections are replaced by one for the database under test, and worker loops end after two rounds
 */
#[CoversClass(DoctrineEventStore::class)]
#[CoversClass(TagPartition::class)]
final class DocumentationExamplesTest extends TestCase
{
    private const TABLE = 'dcb_documentation_test';
    private const ROOT = __DIR__ . '/../..';

    /** @var list<array{event: SequencedEvent, tag: Tag|null}> */
    public static array $handled = [];

    private Connection $connection;

    protected function setUp(): void
    {
        self::$handled = [];
        if (!function_exists('handle')) {
            eval('function handle(' . SequencedEvent::class . ' $event, ?' . Tag::class . ' $tag = null): void { ' . self::class . '::$handled[] = ["event" => $event, "tag" => $tag]; }');
        }
        $this->connection = self::createConnection();
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
    }

    protected function tearDown(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS ' . self::TABLE);
        $this->connection->close();
    }

    public static function createConnection(): Connection
    {
        $dsn = getenv('DCB_TEST_DSN');
        if (!is_string($dsn)) {
            // doctrine/dbal 3.6.0 fails to parse the equivalent "pdo-sqlite:///events_test.sqlite"
            $dsn = 'pdo-sqlite://localhost/events_test.sqlite';
        }
        $config = new Configuration();
        $config->setSchemaManagerFactory(new DefaultSchemaManagerFactory());
        return DriverManager::getConnection((new DsnParser())->parse($dsn), $config);
    }

    #[Test]
    public function quick_start_runs_as_a_standalone_script(): void
    {
        $block = self::codeBlocks('README.md')[0];
        $script = tempnam(sys_get_temp_dir(), 'dcb');
        self::assertIsString($script);
        file_put_contents($script, "<?php\n" . $block['code']);
        try {
            // doctrine/dbal 3.x triggers deprecations for SQLite on PHP 8.5+, which would end up in the compared output
            exec('cd ' . escapeshellarg(self::ROOT) . ' && ' . escapeshellarg(PHP_BINARY) . ' -d error_reporting=' . (E_ALL & ~E_DEPRECATED) . ' ' . escapeshellarg($script) . ' 2>&1', $output, $exitCode);
        } finally {
            unlink($script);
        }
        self::assertSame(0, $exitCode, implode("\n", $output));
        self::assertSame($block['output'], implode("\n", $output) . "\n");
    }

    #[Test]
    public function readme_examples_produce_the_documented_output(): void
    {
        $scope = [];
        foreach (self::codeBlocks('README.md') as $block) {
            [$output, $scope] = self::execute($block['code'], $scope);
            if ($block['output'] !== null) {
                self::assertSame($block['output'], $output, "Output of:\n" . $block['code']);
            }
        }
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE);
        $storedPositions = [];
        foreach ($eventStore->read(Query::all()) as $sequencedEvent) {
            $storedPositions[] = $sequencedEvent->position->value;
        }
        self::assertNotEmpty($storedPositions);
        self::assertSame($storedPositions, array_map(static fn(array $handled) => $handled['event']->position->value, self::$handled));
    }

    #[Test]
    public function partition_example_handles_the_tags_of_its_partition(): void
    {
        $eventStore = DoctrineEventStore::create($this->connection, self::TABLE);
        $eventStore->setup();
        if ($this->connection->getDatabasePlatform() instanceof SQLitePlatform) {
            self::markTestSkipped('Partitioned reads are not supported on SQLite');
        }
        $events = [Event::create('SomeEvent', 'without matching tag', 'other:1')];
        for ($i = 0; $i < 100; $i++) {
            $events[] = Event::create('SomeEvent', 'data', "cart:$i");
        }
        $eventStore->append(Events::fromArray($events));

        $blocks = self::codeBlocks('docs/advanced.md');
        self::assertCount(1, $blocks);
        [, $scope] = self::execute($blocks[0]['code'], ['eventStore' => $eventStore]);

        $partition = TagPartition::create(prefix: 'cart:', partitions: 8, partition: 3);
        $expected = [];
        for ($i = 0; $i < 100; $i++) {
            if ($partition->contains(Tag::fromString("cart:$i"))) {
                $expected[] = "cart:$i";
            }
        }
        self::assertNotEmpty($expected);
        self::assertSame($expected, array_map(static fn(array $handled) => $handled['tag']?->value, self::$handled));
        self::assertEquals($eventStore->head(), $scope['checkpoint']);
    }

    /**
     * @return list<array{code: string, output: string|null}>
     */
    private static function codeBlocks(string $file): array
    {
        $markdown = file_get_contents(self::ROOT . '/' . $file);
        self::assertIsString($markdown);
        preg_match_all('/^```php\n(?<code>.*?)^```\n(?:\n```text\n(?<output>.*?)^```\n)?/ms', $markdown, $matches, PREG_SET_ORDER);
        return array_map(static fn(array $match) => ['code' => $match['code'], 'output' => $match['output'] ?? null], $matches);
    }

    /**
     * @param array<string, mixed> $__scope variables of the previously executed blocks
     * @return array{string, array<string, mixed>} output and variables of the block
     */
    private static function execute(string $__code, array $__scope): array
    {
        $__code = (string) preg_replace('/DriverManager::getConnection\(.*\);/', '\\' . self::class . '::createConnection();', $__code);
        $__code = str_replace(
            ["require 'vendor/autoload.php';", "'events'", 'while (true)', 'waitForNewEvents(30.0)'],
            ['', "'" . self::TABLE . "'", 'foreach ([1, 2] as $round)', 'waitForNewEvents(0.01)'],
            $__code,
        );
        extract($__scope);
        unset($__scope);
        ob_start();
        try {
            eval($__code);
        } catch (Throwable $e) {
            ob_end_clean();
            throw new RuntimeException("Failed to execute:\n$__code\n{$e->getMessage()}", 1793900001, $e);
        }
        $__output = (string) ob_get_clean();
        $__variables = get_defined_vars();
        unset($__variables['__code'], $__variables['__output']);
        return [$__output, $__variables];
    }
}
