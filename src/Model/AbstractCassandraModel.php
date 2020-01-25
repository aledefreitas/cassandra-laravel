<?php
/**
 * @author      Alexandre de Freitas Caetano <alexandrefc2@hotmail.com>
 */

namespace ShSo\Lacassa\Model;

use Illuminate\Database\Eloquent\Model;
use ShSo\Lacassa\Query\Builder as QueryBuilder;
use Vkovic\LaravelCustomCasts\HasCustomCasts;
use ShSo\Lacassa\Model\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Builder;

abstract class AbstractCassandraModel extends Model
{
    use HasCustomCasts;

    /**
     * The connection to use for this model
     *
     * @var string
     */
    protected $connection = 'cassandra';

    /**
     * @var bool
     */
    public $timestamps = false;

    /**
     * @var bool
     */
    public $incrementing = false;

    /**
     * Set the keys for a save update query.
     *
     * @param  \Illuminate\Database\Eloquent\Builder  $query
     *
     * @return \Illuminate\Database\Eloquent\Builder
     */
    protected function setKeysForSaveQuery(Builder $query)
    {
        if (gettype($this->getKeyName()) === 'array') {
            foreach ($this->getKeyName() as $key => $caster) {
                $value = $this->getOriginal($key) ?? $this->getAttribute($key);

                if (isset($caster) and class_exists($caster) and gettype($value) !== 'object') {
                    if (! $value instanceof $caster) {
                        $value = new $caster($value);
                    }
                }

                $query->where($key, '=', $value);
            }
        } else {
            $query->where($this->getKeyName(), '=', $this->getKeyForSaveQuery());
        }

        return $query;
    }

    /**
     * Get a new query builder instance for the connection.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    protected function newBaseQueryBuilder()
    {
        $connection = $this->getConnection();

        return new QueryBuilder(
            $connection,
            $connection->getQueryGrammar(),
            $connection->getPostProcessor()
        );
    }

    /**
     * Create a new Eloquent query builder for the model.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return \ShSo\Lacassa\Model\Eloquent\Builder|static
     */
    public function newEloquentBuilder($query)
    {
        return new EloquentBuilder($query);
    }

    /**
     * Handles list types from Cassandra
     *
     * @param  string  $key
     *
     * @return mixed
     */
    public function __get($key)
    {
        $attribute = $this->getAttribute($key);

        if (is_object($attribute)) {
            switch (get_class($attribute)) {
                case \Cassandra\Map::class:
                    return array_combine($attribute->keys(), $attribute->values());
                    break;

                case \Cassandra\Set::class:
                    return $attribute->values();
                    break;

                case \Cassandra\Collection::class:
                    return $attribute->values();
                    break;
            }
        }

        return parent::__get($key);
    }

    /**
     * Handle dynamic method calls into the model.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        if (in_array($method, ['increment', 'decrement'])) {
            return $this->$method(...$parameters);
        }

        return $this->forwardCallTo($this->on($this->connection), $method, $parameters);
    }

    /**
     * Gets the qualified key name
     *
     * @return string
     */
    public function getQualifiedKeyName()
    {
        if (!$this->primaryKey) {
            throw new \Exception('Primary key not set');
        }

        return $this->primaryKey;
    }
}
