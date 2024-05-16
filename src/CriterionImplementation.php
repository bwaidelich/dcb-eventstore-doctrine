<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreDoctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use Wwwision\DCBEventStore\Types\StreamQuery\Criterion;

interface CriterionImplementation
{
    /**
     * @return class-string<Criterion>
     */
    public function criterionClassName(): string;

    public function applyCriterionConstraints(Criterion $criterion, QueryBuilder $queryBuilder, DoctrineEventStoreConfiguration $config): void;
}
