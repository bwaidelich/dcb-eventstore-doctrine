<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\CriterionImplementations;

use Doctrine\DBAL\Query\QueryBuilder;
use RuntimeException;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesAndTagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;
use Wwwision\DCBEventStoreDoctrine\CriterionImplementation;

final class EventTypesAndTagsCriterionImplementation implements CriterionImplementation
{

    public function criterionClassName(): string
    {
        return EventTypesAndTagsCriterion::class;
    }

    public function applyCriterionConstraints(Criterion $criterion, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        if (!$criterion instanceof EventTypesAndTagsCriterion) {
            throw new RuntimeException(sprintf('%s only supports criteria of type "%s" given: %s', self::class, EventTypesAndTagsCriterion::class, get_debug_type($criterion)), 1700910457);
        }
        EventTypesCriterionImplementation::applyEventTypesConstraints($criterion->eventTypes, $queryBuilder, $config);
        TagsCriterionImplementation::applyTagsConstraints($criterion->tags, $queryBuilder, $config);
    }
}
