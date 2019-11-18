<?php

namespace ShSo\Lacassa\Query;

use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;
use Illuminate\Support\Arr;

class Grammar extends BaseGrammar
{
    protected $selectComponents = [
        'columns',
        'from',
        'wheres',
        'limit',
        'allowFiltering',
    ];

    /**
     * Compile an insert statement into CQL.
     *
     * @param \ShSo\Lacassa\Query $query
     * @param array $values
     *
     * @return string
     */
    public function compileInsert(BaseBuilder $query, array $values)
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the CQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.
        $table = $this->wrapTable($query->from);

        $columns = $this->columnize(array_keys(reset($values)));

        // We need to build a list of parameter place-holders of values that are bound
        // to the query. Each insert should have the exact same amount of parameter
        // bindings so we will loop through the record and parameterize them all.
        $parameters = collect($values)->map(function ($record) {
            return $this->parameterize($record);
        })->implode(', ');

        return "insert into {$table} ({$columns}) values ({$parameters})";
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param string $value
     *
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value !== '*') {
            return str_replace('"', '""', $value);
        }

        return $value;
    }

    /**
     * Compile a delete statement into CQL.
     *
     * @param \ShSo\Lacassa\Query $query
     *
     * @return string
     */
    public function compileDelete(BaseBuilder $query)
    {
        $delColumns = '';
        if (isset($query->delParams)) {
            $delColumns = implode(', ', $query->delParams);
        }

        $wheres = is_array($query->wheres) ? $this->compileWheres($query) : '';
        $allowFiltering = $query->allowFiltering ? 'ALLOW FILTERING' : '';

        return trim("delete {$delColumns} from {$this->wrapTable($query->from)} {$wheres} {$allowFiltering}");
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @param array $values
     *
     * @return string
     */
    public function compileUpdate(BaseBuilder $query, $values)
    {
        $table = $this->wrapTable($query->from);
        // Each one of the columns in the update statements needs to be wrapped in the
        // keyword identifiers, also a place-holder needs to be created for each of
        // the values in the list of bindings so we can make the sets statements.
        $columns = collect($values)->map(
            function ($value, $key) {
                return $this->wrap($key).' = '.$this->parameter($value);
            }
        )->implode(', ');

        // Of course, update queries may also be constrained by where clauses so we'll
        // need to compile the where clauses and attach it to the query so only the
        // intended records are updated by the SQL statements we generate to run.
        $wheres = $this->compileWheres($query);
        $upateCollections = $this->compileUpdateCollections($query);
        if ($upateCollections) {
            $upateCollections = $columns ? ', '.$upateCollections : $upateCollections;
        }

        $allowFiltering = $query->allowFiltering ? 'ALLOW FILTERING' : '';

        return trim("update {$table} set $columns $upateCollections $wheres {$allowFiltering}");
    }

    /**
     * Compiles the udpate collection methods.
     *
     * @param BaseBuilder $query
     *
     * @return string
     */
    public function compileUpdateCollections(BaseBuilder $query)
    {
        $updateCollections = collect($query->updateCollections ?? []);

        $updateCollectionCql = $updateCollections->map(function ($collection, $key) {
            if ($collection['operation']) {
                return "{$collection['column']} = {$collection['column']}{$collection['operation']}?";
            } else {
                return $collection['column'].' = ?';
            }
        })->implode(', ');

        return $updateCollectionCql;
    }

    /**
     * Compiles the values assigned to collections.
     *
     * @param \Cassandra\Value $type
     * @param string $value
     *
     * @return string
     */
    public function compileCollectionValues(\Cassandra\Value $value)
    {
        switch (get_class($value)) {
            case \Cassandra\Map::class:
                return '{' . $this->buildCollectionString($value) . '}';
                break;

            case \Cassandra\Set::class:
                return '{' . $this->buildCollectionString($value) . '}';
                break;

            case \Cassandra\Collection::class:
                return '[' . $this->buildCollectionString($value) . ']';
                break;
        }

        return '{}';
    }

    /**
     * Prepare the bindings for an update statement.
     *
     * @param  array  $bindings
     * @param  array  $values
     * @return array
     */
    public function prepareBindingsForUpdate(array $bindings, array $values)
    {
        $cleanBindings = Arr::except($bindings, ['select', 'join', 'updateCollection']);

        return array_values(
            array_merge(
                $bindings['join'],
                $bindings['updateCollection'],
                $values,
                Arr::flatten($cleanBindings)
            )
        );
    }

    /**
     * Checks if a value type is string or not
     *
     * @param  mixed  $type
     *
     * @return bool
     */
    private function isStringType($type)
    {
        if (is_string($type)) {
            return true;
        }

        switch ($type->type()) {
            case 'varchar':
            case 'ascii':
            case 'inet':
            case 'text':
                return true;
                break;
        }

        return false;
    }

    /**
     * @param Builder $query
     * @param string $columns
     *
     * @return string
     */
    public function compileIndex($query, $columns)
    {
        $table = $this->wrapTable($query->from);
        $value = implode(', ', $columns);

        return 'CREATE INDEX IF NOT EXISTS ON '.$table.'('.$value.')';
    }

    public function compileAllowFiltering($query, $allow_filtering)
    {
        return $allow_filtering ? 'allow filtering' : '';
    }
}
