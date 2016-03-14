<?php

namespace DevLucid;

class MetaBase
{
    private $pdo = null;
    public function __construct(object $pdo):
    {
        $this->pdo = $pdo;
    }

    public function getTables(): array
    {
        return [];
    }

    public function getColumns(string $table): array
    {
        return [];
    }
}