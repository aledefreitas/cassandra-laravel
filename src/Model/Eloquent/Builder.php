<?php
/**
 * @author      Alexandre de Freitas Caetano <alexandrefc2@hotmail.com>
 */

 namespace ShSo\Lacassa\Model\Eloquent;

use Illuminate\Database\Eloquent\Builder as BaseBuilder;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Support\LazyCollection;

class Builder extends BaseBuilder
{
    /**
     * Find a model by its primary key.
     *
     * @param  mixed  $id
     * @param  array  $columns
     * @return \Illuminate\Database\Eloquent\Model|\Illuminate\Database\Eloquent\Collection|static[]|static|null
     */
    public function find($id, $columns = ['*'])
    {
        $idsArray = $id;

        if (gettype(array_pop($idsArray)) === 'array') {
            return $this->findMany($id, $columns);
        }

        return $this->whereKey($id)->first($columns);
    }

    /**
     * Find a model by its primary key or throw an exception.
     *
     * @param  mixed  $id
     * @param  array  $columns
     * @return \Illuminate\Database\Eloquent\Model|\Illuminate\Database\Eloquent\Collection|static|static[]
     *
     * @throws \Illuminate\Database\Eloquent\ModelNotFoundException
     */
    public function findOrFail($id, $columns = ['*'])
    {
        $result = $this->find($id, $columns);

        if (isset($result)) {
            return $result;
        }

        throw (new ModelNotFoundException)->setModel(
            get_class($this->model),
            $id
        );
    }

    /**
     * Add a where clause on the primary key to the query.
     *
     * @param  mixed  $id
     * @return $this
     */
    public function whereKey($id)
    {
        $modelPrimaryKey = $this->model->getKeyName();

        if (gettype($modelPrimaryKey) === 'array') {
            if (!count(array_intersect_key($id, $modelPrimaryKey))) {
                throw new \Exception('No primary key was specified.');
            }

            foreach ($modelPrimaryKey as $key => $caster) {
                if (isset($id[$key])) {
                    $this->where($key, $id[$key]);
                }
            }

            return $this;
        } else {
            return parent::whereKey($id);
        }
    }

    /**
     * Add a where clause on the primary key to the query.
     *
     * @param  mixed  $id
     * @return $this
     */
    public function whereKeyNot($id)
    {
        throw new \Exception('Not implemented in Cassandra. Please use the Query Builder.');
    }

    /**
     * Get the hydrated models without eager loading.
     *
     * @param  array  $columns
     * @return \Illuminate\Database\Eloquent\Model[]|static[]
     */
    public function getModels($columns = ['*'])
    {
        $models = LazyCollection::make(function () use ($columns) {
            $results = $this->query->getCassandraRows($columns);

            do {
                foreach ($results as $row) {
                    yield $row;
                }
            } while (!$results->isLastPage() and $results = $results->nextPage());
        })->all();

        return $this->model->hydrate($models)->all();
    }

    /**
     * Get async
     *
     * @param  array  $columns
     *
     * @return \Cassandra\FutureRows
     */
    public function getAsync(array $columns = [ '*' ])
    {
        return $this->forwardCallTo($this->query, 'getAsync', [ $columns ]);
    }

    /**
     * Get cassandra rows
     *
     * @param  array  $columns
     *
     * @return \Cassandra\Rows
     */
    public function getCassandraRows(array $columns = [ '*' ])
    {
        return $this->forwardCallTo($this->query, 'getCassandraRows', [ $columns ]);
    }

    /**
     * {@inheritDoc}
     */
    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null)
    {
        return $this->forwardCallTo($this->query, 'paginate', [ $perPage, $columns, $pageName, $page ]);
    }
}
