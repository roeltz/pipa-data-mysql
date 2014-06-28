<?php

namespace Pipa\Data\Source\MySQL;
use DateTime;
use DateTimeZone;
use Pipa\Data\Collection;
use Pipa\Data\Field;
use Pipa\Data\Util\GenericSQLGenerator;

class MySQLGenerator extends GenericSQLGenerator {

	protected $dataSource;

	function __construct(MySQLDataSource $dataSource) {
		$this->dataSource = $dataSource;
	}

	function escapeField(Field $field) {
		$escaped = $this->escapeIdentifier($field->name);
		if ($field->collection) {
			$escaped = $this->escapeIdentifier(
				$field->collection->alias
				? $field->collection->alias
				: $field->collection->name
			).".$escaped";
		}
		return $escaped;
	}

	function escapeIdentifier($name) {
		return "`$name`";
	}

	function escapeValue($value) {
		if (is_string($value)) {
			return "'".$this->dataSource->getConnection()->escape_string($value)."'";
		} elseif ($value instanceof DateTime) {
			if ($value->getOffset() != 0) {
				$value = clone $value;
				$value->setTimezone(new DateTimeZone("UTC"));
			}
			return $this->escapeValue($value->format('Y-m-d H:i:s'));
		} elseif (is_bool($value))
			return $value ? "TRUE" : "FALSE";
		elseif (is_null($value))
			return "NULL";
		elseif (is_object($value))
			return $this->escapeValue((string) $value);
		else
			return $value;
	}

	function renderRegex($a, $b) {
		return "$a REGEXP $b";
	}
}
