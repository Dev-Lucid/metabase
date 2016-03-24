<?php

namespace Lucid\Library\Metabase;

class MetaBase
{
    private $pdo = null;
    private $driver = null;
    private $schema = null;
    public function __construct(\PDO $pdo)
    {
        $this->pdo = $pdo;
        $this->driver = $this->pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
    }

    public function setSchema(string $newSchema)
    {
        $this->schema = $newSchema;
    }

    public function getTables(bool $includeViews=true): array
    {
        $tables = [];

        switch ($this->driver) {
            case 'sqlite':
                $query = "SELECT name FROM sqlite_master WHERE type in ('table'". (($includeViews === true)?",'view'":'').");";
                $statement = $this->pdo->query($query);
                return $statement->fetchAll(\PDO::FETCH_COLUMN);
                break;
            case 'pgsql':
                $query = "select table_name from information_schema.tables where table_schema='".$this->schema."' and  table_type in ('BASE TABLE'". (($includeViews === true)?",'VIEW'":'').");";
                $statement = $this->pdo->query($query);
                return $statement->fetchAll(\PDO::FETCH_COLUMN);
                break;
        }
        return $tables;
    }

    public function getViews(): array
    {
        $views = [];

        switch ($this->driver) {
            case 'sqlite':
                $query = "SELECT name FROM sqlite_master WHERE type in ('view');";
                $statement = $this->pdo->query($query);
                return $statement->fetchAll(\PDO::FETCH_COLUMN);
                break;
            case 'pgsql':
                $query = "select table_name from information_schema.tables where table_schema='".$this->schema."' and  table_type in ('VIEW');";
                $statement = $this->pdo->query($query);
                return $statement->fetchAll(\PDO::FETCH_COLUMN);
                break;

        }
        return $views;
    }

    public function getColumns(string $name): array
    {


        switch ($this->driver) {
            case 'sqlite':
                $isTable = $this->isTable($name);
                $isView  = false;

                if ($isTable === true) {
                    $query = "PRAGMA table_info($name);";
                    $statement = $this->pdo->query($query);
                    return $this->standardizeColumns($statement->fetchAll());
                } else {
                    $isView = $this->isView($name);
                    if($isView === true){
                        $query = "select * from $name limit 1;";
                        $statement = $this->pdo->query($query);
                        $columns = $statement->fetch(\PDO::FETCH_ASSOC);

                        $finalCols = [];
                        $index = 0;
                        foreach ($columns as $key=>$value) {
                            $newCol = [
                                'index'=>null,
                                'name'=>null,
                                'type'=>null,
                                'default'=>null,
                                'notnull'=>null,
                                'max'=>null,
                            ];
                            $newCol['index'] = $index;
                            $newCol['name'] = $key;

                            if (is_string($value)) {
                                $newCol['type'] = 'string';
                            } elseif (is_int($value)) {
                                $newCol['type'] = 'int';
                            } elseif (is_float($value)) {
                                $newCol['type'] = 'float';
                            } elseif (is_bool($value)) {
                                $newCol['type'] = 'bool';
                            } else {
                                $newCol['type'] = 'string';
                            }
                            $finalCols[] = $newCol;
                            $index++;
                        }

                        return $finalCols;

                    } else {
                        throw new \Exception('Could not find table or view named '.$name);
                    }
                }
                break;
            case 'pgsql':
                $query = "select * from information_schema.columns where table_schema='".$this->schema."' and table_name='$name' order by ordinal_position;";
                $statement = $this->pdo->query($query);
                return $this->standardizeColumns($statement->fetchAll());
                break;
        }

        return [];
    }

    public function isTable(string $name, bool $includeViews=false): bool
    {
        $tables = $this->getTables($includeViews);
        return (in_array($name, $tables) === true);
    }

    public function isView(string $name): bool
    {
        $views = $this->getViews();
        return (in_array($name, $views) === true);
    }

    private function standardizeColumns($columns): array
    {
        $finalCols = [];

        foreach ($columns as $column) {

            $newCol = [
                'index'=>null,
                'name'=>null,
                'type'=>null,
                'default'=>null,
                'notnull'=>null,
                'max'=>null,
            ];

            switch ($this->driver) {
                case 'sqlite':
                    $newCol['index'] = $column['cid'];
                    $newCol['name'] = $column['name'];
                    $newCol['notnull'] = ($column['notnull'] == 1);
                    $newCol['default'] = $column['dflt_value'];
                    list($newCol['type'], $newCol['max']) = $this->getTypeMax($column['type']);

                    # fix the default for booleans to be a php true/false, not a 1/0
                    if ($newCol['type'] == 'bool' && ($column['dflt_value'] == 0 || $column['dflt_value'] == 1)) {
                        $newCol['default'] = ($column['dflt_value'] == 1);
                    }

                    # fix the default for dates if set to now() to CURRENT_TIMESTAMP for consistency
                    if ($newCol['type'] == 'date' && $column['dflt_value'] == 'now()') {
                        $newCol['default'] = 'CURRENT_TIMESTAMP';
                    }

                    break;
                case 'pgsql':
                    $newCol['index'] = $column['ordinal_position'];
                    $newCol['name'] = $column['column_name'];
                    $newCol['notnull'] = ($column['is_nullable'] == 'NO');
                    $newCol['default'] = $column['column_default'];

                    list($newCol['type'], $newCol['max']) = $this->getTypeMax($column['data_type']);
                    break;
            }

            $finalCols[] = $newCol;
        }

        return $finalCols;
    }

    private function getTypeMax($type): array
    {
        $max = null;

        $type = strtolower($type);
        $type = str_replace(')', '', $type);
        $type = str_replace('(', '|', $type);
        $parts = explode('|', $type);
        $type = array_shift($parts);
        $type = trim($type);

        if (count($parts) > 0) {
            $max = trim(array_shift($parts));
        }

        switch ($type) {
            case 'int':
            case 'bigint':
            case 'tinyint':
            case 'serial':
            case 'integer':
                return ['int', $max];
                break;
            case 'numeric':
            case 'decimal':
                return ['float', $max];
                break;
            case 'varchar':
            case 'character varying':
            case 'text':
            case 'char':
            case 'character':
                return ['string', $max];
                break;
            case 'datetime':
            case 'date':
            case 'timestamp':
            case 'timestamp with time zone':
            case 'timestamp without time zone':
                return ['timestamp', null];
                break;
            case 'bool':
            case 'boolean':
                return ['bool', null];
            default:
                return ['unknown('.$type.')', 999];
                break;
        }
    }
}