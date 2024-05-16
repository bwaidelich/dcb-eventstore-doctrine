<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine\CriterionImplementations;

use Doctrine\DBAL\Query\QueryBuilder;
use RuntimeException;
use Wwwision\DCBEventStore\Types\StreamQuery\Criteria\TagsCriterion;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;
use Wwwision\DCBEventStore\Types\Tags;
use Wwwision\DCBEventStoreDoctrine\DoctrineEventStoreConfiguration;
use Wwwision\DCBEventStoreDoctrine\CriterionImplementation;

final class TagsCriterionImplementation implements CriterionImplementation
{

    public function criterionClassName(): string
    {
        return TagsCriterion::class;
    }

    public function applyCriterionConstraints(Criterion $criterion, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        if (!$criterion instanceof TagsCriterion) {
            throw new RuntimeException(sprintf('%s only supports criteria of type "%s" given: %s', self::class, TagsCriterion::class, get_debug_type($criterion)), 1700910126);
        }
        self::applyTagsConstraints($criterion->tags, $queryBuilder, $config);
    }

    public static function applyTagsConstraints(Tags $tags, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void
    {
        $tagsParameterName = $config->createUniqueParameterName();
        if ($config->isSQLite()) {
            $queryBuilder->andWhere("NOT EXISTS(SELECT value FROM JSON_EACH(:$tagsParameterName) WHERE value NOT IN (SELECT value FROM JSON_EACH(events.tags)))");
        } elseif ($config->isPostgreSQL()) {
            $queryBuilder->andWhere("events.tags @> :$tagsParameterName::jsonb");
        } else {
            $queryBuilder->andWhere("JSON_CONTAINS(tags, :$tagsParameterName)");
        }
        $queryBuilder->setParameter($tagsParameterName, json_encode($tags));
    }
}
