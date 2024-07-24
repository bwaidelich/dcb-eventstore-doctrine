<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\CriterionImplementations;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
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
        if ($criterion->eventTypes !== null) {
            $eventTypesParameterName = $config->createUniqueParameterName();
            $queryBuilder->andWhere("type IN (:$eventTypesParameterName)");
            $queryBuilder->setParameter($eventTypesParameterName, $criterion->eventTypes->toStringArray(), class_exists(ArrayParameterType::class) ? ArrayParameterType::STRING : Connection::PARAM_STR_ARRAY);
        }
        if ($criterion->tags !== null) {
            $tagsParameterName = $config->createUniqueParameterName();
            if ($config->isSQLite()) {
                $queryBuilder->andWhere("NOT EXISTS(SELECT value FROM JSON_EACH(:$tagsParameterName) WHERE value NOT IN (SELECT value FROM JSON_EACH(events.tags)))");
            } elseif ($config->isPostgreSQL()) {
                $queryBuilder->andWhere("events.tags @> :$tagsParameterName::jsonb");
            } else {
                $queryBuilder->andWhere("JSON_CONTAINS(tags, :$tagsParameterName)");
            }
            $queryBuilder->setParameter($tagsParameterName, json_encode($criterion->tags));
        }
        if ($criterion->onlyLastEvent) {
            $queryBuilder->select('MAX(sequence_number) AS sequence_number, \'' . $criterion->hash()->value . '\' AS criterion_hash');
        }
    }
}
