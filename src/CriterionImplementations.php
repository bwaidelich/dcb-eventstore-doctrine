<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use InvalidArgumentException;
use RuntimeException;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;
use Wwwision\DCBEventStoreDoctrine\CriterionImplementations\EventTypesAndTagsCriterionImplementation;

final class CriterionImplementations
{
    /**
     * @param array<class-string<Criterion>, CriterionImplementation> $criterionImplementationsByCriterionClassName
     */
    private function __construct(
        private readonly array $criterionImplementationsByCriterionClassName
    ) {
    }

    public static function createDefault(): self
    {
        return self::create(
            new EventTypesAndTagsCriterionImplementation(),
        );
    }

    public static function create(CriterionImplementation ...$implementations): self
    {
        $criterionImplementationsByCriterionClassName = [];
        foreach ($implementations as $implementation) {
            if (array_key_exists($implementation->criterionClassName(), $criterionImplementationsByCriterionClassName)) {
                throw new InvalidArgumentException(sprintf('Failed to create instance of %s because there are multiple implementations for criterion of type "%s" registered', self::class, $implementation->criterionClassName()), 1700909489);
            }
            $criterionImplementationsByCriterionClassName[$implementation->criterionClassName()] = $implementation;
        }
        return new self($criterionImplementationsByCriterionClassName);
    }

    public function with(CriterionImplementation $implementation): self
    {
        return self::create(...[...array_values($this->criterionImplementationsByCriterionClassName), $implementation]);
    }

    public function applyCriterionConstraints(Criterion $criterion, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        if (!array_key_exists($criterion::class, $this->criterionImplementationsByCriterionClassName)) {
            throw new RuntimeException(sprintf('Failed to apply constraints for criterion of type "%s" because no corresponding implementation is registered. Available criterion types: "%s"', $criterion::class, implode('", "', array_keys($this->criterionImplementationsByCriterionClassName))), 1700909681);
        }
        $this->criterionImplementationsByCriterionClassName[$criterion::class]->applyCriterionConstraints($criterion, $queryBuilder, $config);
    }
}
