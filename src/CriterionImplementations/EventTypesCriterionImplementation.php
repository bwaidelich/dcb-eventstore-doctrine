<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\CriterionImplementations;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Query\QueryBuilder;
use RuntimeException;
use Wwwision\DCBEventStore\Types\EventTypes;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\EventTypesCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;
use Wwwision\DCBEventStoreDoctrine\CriterionImplementation;

final class EventTypesCriterionImplementation implements CriterionImplementation
{

    public function criterionClassName(): string
    {
        return EventTypesCriterion::class;
    }

    public function applyCriterionConstraints(Criterion $criterion, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        if (!$criterion instanceof EventTypesCriterion) {
            throw new RuntimeException(sprintf('%s only supports criteria of type "%s" given: %s', self::class, EventTypesCriterion::class, get_debug_type($criterion)), 1700910318);
        }
        self::applyEventTypesConstraints($criterion->eventTypes, $queryBuilder, $config);
    }

    public static function applyEventTypesConstraints(EventTypes $eventTypes, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        $eventTypesParameterName = $config->createUniqueParameterName();
        $queryBuilder->andWhere("type IN (:$eventTypesParameterName)");
        $queryBuilder->setParameter($eventTypesParameterName, $eventTypes->toStringArray(), ArrayParameterType::STRING);
    }
}
