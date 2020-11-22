<?php

declare(strict_types=1);

namespace Bavix\LaravelClickHouse\Database\Query;

use Bavix\LaravelClickHouse\Database\Connection;
use Closure;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Traits\Macroable;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\ClickhouseBuilder\Query\BaseBuilder;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;

class Builder extends BaseBuilder
{
    use  Macroable {
        __call as macroCall;
    }

    protected $connection;

    public function __construct(
        Connection $connection,
        Grammarr $grammar
    ) {
        $this->connection = $connection;
        $this->grammar = $grammar;
    }

    /**
     * Perform compiled from builder sql query and getting result.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return Collection
     */
    public function get(): Collection
    {
        if (!empty($this->async)) {
            $result = $this->connection->selectAsync($this->toAsyncSqls());
        } else {
            $result = $this->connection->select($this->toSql(), [], $this->getFiles());
        }

        return collect($result);
    }

    /**
     * Performs compiled sql for count rows only. May be used for pagination
     * Works only without async queries.
     *
     * @param string $column Column to pass into count() aggregate function
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return int
     */
    public function count($column = '*'): int
    {
        $builder = $this->getCountQuery($column);
        $result = $builder->get();

        if (count($this->groups) > 0) {
            return count($result);
        }

        return (int) ($result[0]['count'] ?? 0);
    }

    /**
     * Perform query and get first row.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return mixed|null
     */
    public function first()
    {
        return $this->get()->first();
    }

    /**
     * Makes clean instance of builder.
     *
     * @return self
     */
    public function newQuery(): self
    {
        return new static($this->connection, $this->grammar);
    }

    /**
     * Insert in table data from files.
     *
     * @param array  $columns
     * @param array  $files
     * @param string $format
     * @param int    $concurrency
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return array
     */
    public function insertFiles(array $columns, array $files, string $format = Format::CSV, int $concurrency = 5): array
    {
        return $this->connection->insertFiles(
            (string) $this->getFrom()->getTable(),
            $columns,
            $files,
            $format,
            $concurrency
        );
    }

    /**
     * Performs insert query.
     *
     * @param array $values
     *
     * @return bool
     */
    public function insert(array $values): bool
    {
        if (empty($values)) {
            return false;
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        // Here, we will sort the insert keys for every record so that each insert is
        // in the same order for the record. We need to make sure this is the case
        // so there are not any errors or problems when inserting these records.
        foreach ($values as $key => &$value) {
            ksort($value);
        }

        return $this->connection->insert(
            $this->grammar->compileInsert($this, $values),
            Arr::flatten($values)
        );
    }

    /**
     * @return Connection
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }
	
	/**
	 * @param             $table
	 * @param string|null $strict
	 * @param string|null $type
	 * @param array|null  $using
	 * @param bool        $global
	 * @param string|null $alias
	 *
	 * @return $this
	 */
	public function joinRaw( $table, string $strict = null, string $type = null, array $using = null, bool $global = false, ?string $alias = null )
	{
		$this->join = new JoinClause($this);
		
		/*
		 * If builder instance given, then we assume that sub-query should be used as table in join
		 */
		if ($table instanceof BaseBuilder) {
			$this->join->query($table);
			
			$this->files = array_merge($this->files, $table->getFiles());
		}
		
		/*
		 * If closure given, then we call it and pass From object as argument to
		 * set up JoinClause object in callback
		 */
		if ($table instanceof Closure) {
			$table($this->join);
		}
		
		/*
		 * If given anything that is not builder instance or callback. For example, string,
		 * then we assume that table name was given.
		 */
		if (!$table instanceof Closure && !$table instanceof BaseBuilder) {
			$this->join->table($table);
		}
		
		/*
		 * If using was given, then merge it with using given before, in closure
		 */
		if (!is_null($using)) {
			$this->join->addUsing($using);
		}
		
		if (!is_null($strict) && is_null($this->join->getStrict())) {
			$this->join->strict($strict);
		}
		
		if (!is_null($type) && is_null($this->join->getType())) {
			$this->join->type($type);
		}
		
		if (!is_null($alias) && is_null($this->join->getAlias())) {
			$this->join->as($alias);
		}
		
		$this->join->distributed($global);
		
		if (!is_null($this->join->getSubQuery())) {
			$this->join->query($this->join->getSubQuery());
		}
		
		return $this;
	}
}
