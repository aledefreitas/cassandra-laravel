<?php

namespace ShSo\Lacassa\Query;

use Cassandra;
use Illuminate\Support\Arr;
use ShSo\Lacassa\Connection;
use InvalidArgumentException;
use Illuminate\Database\Query\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /**
     * The current query value bindings.
     *
     * @var array
     */
    public $bindings = [
        'select' => [],
        'where'  => [],
        'updateCollection' => [],
        'join' => [],
    ];

    public $allowFiltering = false;

    public $distinct = false;

    /**
     * The where constraints for the query.
     *
     * @var array
     */
    public $updateCollections = [];

    /**
     * The where constraints for the query.
     *
     * @var array
     */
    public $insertCollections = [];

    /**
     * @var array
     */
    protected $options = [];

    /**
     * @var bool
     */
    protected $useCollection = true;

    /**
     * All of the available clause operators.
     *
     * @var array
     */
    public $operators = [
        '=',
        '<',
        '>',
        '<=',
        '>=',
        'like',
        'contains',
        'contains key',
    ];

    /**
     * Operator conversion.
     *
     * @var array
     */
    protected $conversion = [
        '=' => '$eq',
        '!=' => '$ne',
        '<>' => '$ne',
        '<' => '$lt',
        '<=' => '$lte',
        '>' => '$gt',
        '>=' => '$gte',
    ];

    /**
     * @var array
     */
    public $collectionTypes = [
        \Cassandra\Set::class,
        \Cassandra\Collection::class,
        \Cassandra\Map::class,
    ];

    /**
     * @param Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->grammar = $connection->getQueryGrammar();
        $this->connection = $connection;
    }

    public function distinct()
    {
        $this->distinct = true;

        return $this;
    }

    public function allowFiltering()
    {
        $this->allowFiltering = true;

        return $this;
    }

    /**
     * Sets the options for the query
     *
     * @param  array  $options
     *
     * @return self
     */
    public function withOptions(array $options = [])
    {
        $this->options = $options;

        return $this;
    }

    /**
     * Toggles the use collection option
     *
     * @param  bool  $useCollection
     *
     * @return self
     */
    public function setUseCollection(bool $useCollection)
    {
        $this->useCollection = $useCollection;

        return $this;
    }

    /**
     * Set the table which the query is targeting.
     *
     * @param string $table
     *
     * @return $this
     */
    public function from($collection)
    {
        return parent::from($collection);
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     *
     * @return Cassandra\Rows
     */
    public function get($columns = ['*'])
    {
        if (is_null($this->columns)) {
            $this->columns = $columns;
        }
        $cql = $this->grammar->compileSelect($this);

        return collect($this->execute($cql));
    }

    /**
     * Execute the query as a "select" statement and returns cassandra rows.
     *
     * @param array $columns
     *
     * @return Cassandra\Rows
     */
    public function getCassandraRows($columns = ['*'])
    {
        if (is_null($this->columns)) {
            $this->columns = $columns;
        }
        $cql = $this->grammar->compileSelect($this);

        return $this->execute($cql);
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     *
     * @return Cassandra\FutureRows
     */
    public function getAsync($columns = ['*'])
    {
        if (is_null($this->columns)) {
            $this->columns = $columns;
        }
        $cql = $this->grammar->compileSelect($this);

        return $this->executeAsync($cql);
    }

    /**
     * Execute the CQL query.
     *
     * @param string $cql
     *
     * @return Cassandra\Rows
     */
    private function execute($cql)
    {
        return $this->connection->execute($cql, array_merge($this->options, ['arguments' => $this->getBindings()]));
    }

    /**
     * Execute the CQL query asyncronously.
     *
     * @param string $cql
     *
     * @return Cassandra\FutureRows
     */
    private function executeAsync($cql)
    {
        return $this->connection->executeAsync($cql, array_merge($this->options, ['arguments' => $this->getBindings()]));
    }

    /**
     * Delete a record from the database.
     *
     * @return Cassandra\Rows
     */
    public function deleteRow()
    {
        $query = $this->grammar->compileDelete($this);

        return $this->executeAsync($query);
    }

    /**
     * Delete a column from the database.
     *
     * @param array $columns
     *
     * @return Cassandra\Rows
     */
    public function deleteColumn($columns)
    {
        $this->delParams = $columns;
        $query = $this->grammar->compileDelete($this);

        return $this->executeAsync($query);
    }

    /**
     * Retrieve the "count" result of the query.
     *
     * @param string $columns
     *
     * @return Cassandra\Rows
     */
    public function count($columns = '*')
    {
        $count = 0;
        $result = $this->get(array_wrap($columns));
        while (true) {
            $count += $result->count();
            if ($result->isLastPage()) {
                break;
            }
            $result = $result->nextPage();
        }

        return $count;
    }

    /**
     * Used to update the colletions like set, list and map.
     *
     * @param string $column
     * @param string $operation
     * @param \Cassandra\Value  $value
     *
     * @return string
     */
    public function updateCollection($column, $operation = null, \Cassandra\Value $value = null)
    {
        //Check if the type is anyone in SET, LIST or MAP. else throw ERROR.
        if (!in_array(get_class($value), $this->collectionTypes)) {
            throw new InvalidArgumentException(
                "Invalid binding type: {$type}, Should be any one of ".implode(', ', $this->collectionTypes)
            );
        }

        // Here we will make some assumptions about the operator. If only 2 values are
        // passed to the method, we will assume that the operator is an equals sign
        // and keep going. Otherwise, we'll require the operator to be passed in.
        if (func_num_args() == 2) {
            $value = $operation;
            $operation = null;
        }

        $updateCollection = compact('column', 'value', 'operation');
        $this->updateCollections[] = $updateCollection;

        $this->addCollectionBinding($updateCollection, 'updateCollection');

        return $this;
    }

    /**
     * Gets the collection type
     *
     * @param  \Cassandra\Value  $collection
     *
     * @return string
     */
    private function getCollectionType(\Cassandra\Value $collection)
    {
        switch (get_class($collection)) {
            case \Cassandra\Map::class:
                return 'map';
                break;
            case \Cassandra\Collection::class:
                return 'list';
                break;
            case \Cassandra\Set::class:
                return 'set';
                break;
        }

        return '';
    }

    /**
     * Add a binding to the query.
     *
     * @param array  $value
     * @param string $type
     *
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function addCollectionBinding(array $value, $type = 'updateCollection')
    {
        $value['type'] = $this->getCollectionType($value['value']);

        if (!array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }

        $this->bindings[$type][] = $value['value'];

        return $this;
    }

    /**
     * Update a record in the database.
     *
     * @param array $values
     *
     * @return int
     */
    public function update(array $values = [])
    {
        $cql = $this->grammar->compileUpdate($this, $values);

        return $this->connection->update($cql, $this->cleanBindings(
            $this->grammar->prepareBindingsForUpdate($this->bindings, $values)
        ));
    }

    /**
     * Insert a new record into the database.
     *
     * @param  array  $values
     * @param  bool  $isAsync
     *
     * @return mixed
     */
    public function insert(array $values = [], $isAsync = false)
    {
        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient when building these
        // inserts statements by verifying these elements are actually an array.
        if (empty($values)) {
            return true;
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            // Here, we will sort the insert keys for every record so that each insert is
            // in the same order for the record. We need to make sure this is the case
            // so there are not any errors or problems when inserting these records.
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $cqlStatement = $this->grammar->compileInsert($this, $values);
        $bindings = $this->cleanBindings(Arr::flatten($values, 1));

        // Finally, we will run this query against the database connection and return
        // the results. We will need to also flatten these bindings before running
        // the query so they are all in one huge, flattened array for execution.
        if ($isAsync === true) {
            return $this->connection->insertAsync(
                $cqlStatement,
                $bindings
            );
        }

        return $this->connection->insert(
            $cqlStatement,
            $bindings
        );
    }

    /**
     * Insert a new record into the database asynchronously.
     *
     * @param  array  $values
     *
     * @return bool
     */
    public function insertAsync(array $values = [])
    {
        return $this->insert($values, true);
    }

    /**
     * @param array $columns
     *
     * @return Cassandra\Rows
     */
    public function index($columns = [])
    {
        $cql = $this->grammar->compileIndex($this, $columns);

        return $this->execute($cql);
    }

    /**
     * Paginates results
     *
     * @param  int  $perPage
     * @param  array  $columns
     * @param  string  $page
     *
     * @return \Cassandra\Rows
     */
    public function paginate(
        $perPage = 15,
        $columns = [ '*' ],
        $pageName = 'page',
        $page = null
    ) {
        if (is_null($this->columns)) {
            $this->columns = $columns;
        }

        $cql = $this->grammar->compileSelect($this);

        $options = [
            'arguments' => $this->getBindings(),
            'page_size' => $perPage,
        ];

        if (isset($page)) {
            $options['paging_state_token'] = $page;
        }

        return $this->connection->execute($cql, $options);
    }
}
