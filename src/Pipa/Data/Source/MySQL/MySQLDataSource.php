<?php

namespace Pipa\Data\Source\MySQL;
use DateTime;
use mysqli;
use mysqli_result;
use Pipa\Data\Aggregate;
use Pipa\Data\Collection;
use Pipa\Data\Criteria;
use Pipa\Data\DataSource;
use Pipa\Data\Exception\AuthException;
use Pipa\Data\Exception\ConnectionException;
use Pipa\Data\Exception\ConstraintException;
use Pipa\Data\Exception\DataException;
use Pipa\Data\Exception\DuplicateEntryException;
use Pipa\Data\Exception\InvalidHostException;
use Pipa\Data\Exception\QueryException;
use Pipa\Data\Exception\QuerySyntaxException;
use Pipa\Data\Exception\UnknownCollectionException;
use Pipa\Data\Exception\UnknownHostException;
use Pipa\Data\Exception\UnknownSchemaException;
use Pipa\Data\JoinableCollection;
use Pipa\Data\MultipleInsertionSupport;
use Pipa\Data\RelationalCriteria;
use Pipa\Data\SQLDataSource;
use Pipa\Data\TransactionalDataSource;
use Pipa\Data\Util\AbstractConvenientSQLDataSource;
use Psr\Log\LoggerInterface;

class MySQLDataSource extends AbstractConvenientSQLDataSource implements DataSource, TransactionalDataSource, MultipleInsertionSupport {

	const TYPE_TINYINT = 1;
	const TYPE_SMALLINT = 2;
	const TYPE_MEDIUMINT = 9;
	const TYPE_INT = 3;
	const TYPE_BIGINT = 8;
	const TYPE_DECIMAL = 246;
	const TYPE_FLOAT = 4;
	const TYPE_DOUBLE = 5;
	const TYPE_BIT = 16;
	const TYPE_DATE = 10;
	const TYPE_DATETIME = 12;
	const TYPE_TIMESTAMP = 7;
	const TYPE_TIME = 11;
	const TYPE_YEAR = 13;
	const TYPE_CHAR = 254;
	const TYPE_VARCHAR = 253;
	const TYPE_TEXT = 252;

	protected $connection;
	protected $generator;
	protected $logger;

	function __construct($db, $host, $user, $password) {
		$this->connection = @new mysqli("p:$host", $user, $password, $db);

		if (!$this->connection->connect_errno) {
			$this->generator = new MySQLGenerator($this);
			$this->connection->set_charset("utf8");
			$this->connection->autocommit(true);
		} else {
			throw $this->translateException($this->connection->connect_errno, $this->connection->connect_error);
		}
	}

	function setLogger(LoggerInterface $logger) {
		$this->logger = $logger;
	}

	function aggregate(Aggregate $aggregate, Criteria $criteria) {
		$result = $this->query($this->generator->generateAggregate($aggregate, $criteria));
		return current(current($result));
	}

	function beginTransaction() {
		$this->execute("START TRANSACTION");
	}

	function commit() {
		$this->execute("COMMIT");
	}

	function count(Criteria $criteria) {
		$result = $this->query($this->generator->generateCount($criteria));
		return current(current($result));
	}

	function delete(Criteria $criteria) {
		return $this->execute($this->generator->generateDelete($criteria));
	}

	function execute($sql, array $parameters = null) {
		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);

		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}

		if ($this->connection->query($sql)) {
			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$this->logger->debug("{$this->connection->affected_rows} affected row(s), took {$elapsed}s");
			}

			return $this->connection->affected_rows;
		} else {
			throw $this->translateException($this->connection->errno, $this->connection->error);
		}
	}

	function find(Criteria $criteria) {
		return $this->query($this->generator->generateSelect($criteria));
	}

	function getCollection($name) {
		return new JoinableCollection($name);
	}

	function getConnection() {
		return $this->connection;
	}

	function getCriteria() {
		return new RelationalCriteria($this);
	}

	function query($sql, array $parameters = null) {

		if ($parameters) $sql = $this->generator->interpolateParameters($sql, $parameters);

		if ($this->logger) {
			$this->logger->debug("$sql");
			$start = microtime(true);
		}

		if ($result = $this->connection->query($sql)) {
			$types = $this->resolveResultTypes($result);
			$items = array();
			while ($item = $result->fetch_assoc()) {
				$this->processItem($item, $types);
				$items[] = $item;
			}

			if ($this->logger) {
				$elapsed = microtime(true) - $start;
				$count = count($items);
				$this->logger->debug("Query returned $count item(s), took {$elapsed}s");
			}

			return $items;
		} else {
			throw $this->translateException($this->connection->errno, $this->connection->error);
		}
	}

	function rollback() {
		$this->execute("ROLLBACK");
	}

	function save(array $values, Collection $collection, $sequence = null) {
		$this->execute($this->generator->generateInsert($values, $collection));
		return $this->connection->insert_id;
	}

	function saveMultiple(array $values, Collection $collection) {
		$this->execute($this->generator->generateMultipleInsert($values, $collection));
	}

	function update(array $values, Criteria $criteria) {
		return $this->execute($this->generator->generateUpdate($values, $criteria));
	}

	protected function processItem(array &$items, array &$types) {
		foreach($items as $field=>&$value) {
			if (!is_null($value)) {
				switch($types[$field]) {
					case self::TYPE_TINYINT:
					case self::TYPE_SMALLINT:
					case self::TYPE_MEDIUMINT:
					case self::TYPE_INT:
					case self::TYPE_BIGINT:
					case self::TYPE_YEAR:
						$value = (int) $value;
						continue 2;
					case self::TYPE_DOUBLE:
					case self::TYPE_FLOAT:
					case self::TYPE_DECIMAL:
						$value = (double) $value;
						continue 2;
					case self::TYPE_DATE:
					case self::TYPE_DATETIME:
					case self::TYPE_TIMESTAMP:
						$value = new DateTime($value);
						continue 2;
					case self::TYPE_TIME:
						$value = new DateTime("1970-01-01 $value");
						continue 2;
				}
			}
		}
	}

	protected function resolveResultTypes(mysqli_result $result) {
		$types = array();
		foreach($result->fetch_fields() as $meta)
			$types[$meta->name] = $meta->type;
		return $types;
	}

	protected function translateException($code, $message) {
		if ($this->logger)
			$this->logger->error($message);

		switch($code) {
			case 1044:
			case 1045:
				return new AuthException($message, $code);
			case 1049:
				return new UnknownSchemaException($message, $code);
			case 1062:
				return new DuplicateEntryException($message, $code);
			case 1064:
				return new QuerySyntaxException($message, $code);
			case 1146:
				return new UnknownCollectionException($message, $code);
			case 1452:
				return new ConstraintException($message, $code);
			case 2002:
				return new UnknownHostException($message, $code);
			default:
				return new DataException($message, $code);
		}
	}
}
