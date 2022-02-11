<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse;

use Skeleton\ConnectorDb\Manager\QueryBuilder;

class ClickHouseQueryBuilder implements QueryBuilder {
    private $_select = [];
    private $_from;
    private $_where = [];

    public function __construct($configString) {
        $config = json_decode($configString, TRUE);
        if (!$config) {
            return;
        }
        if (!empty($config['select'])) {
            $this->_select = $config['select'];
        }
        if (!empty($config['from'])) {
            $this->_from = $config['from'];
        }
        if (!empty($config['where'])) {
            $this->_where = $config['where'];
        }
    }

    public function toString() {
        if (empty($this->_select) || empty($this->_from)) {
            throw new ClickHouseConnectorException('Missing select columns and table to select');
        }
        $table = $this->_from;
        $selects = [];
        $groups = [];
        $where = [];

        foreach ($this->_select as $select) {
            $column = '';
            if (!empty($select['function'])) {
                $column = "{$select['function']}(`{$select['field']}`)";
            } else {
                $column = "`{$select['field']}`";
                $groups[] = "`{$select['field']}`";
            }
            if ($select['alias']) {
                $column .= " AS `{$select['alias']}`";
            }
            $selects[] = $column;
        }

        foreach ($this->_where as $row) {
            $whereRow = [];
            foreach ($row as $item) {
                if (isset($item['raw'])) {
                    $whereRow[] = $item['raw'];
                } else if ($item['op'] == 'IN' || $item['op'] == 'NOT IN') {
                    $values = explode(',', $item['value']);
                    $values = array_map(function ($value) {
                        $value = trim($value);
                        if (
                            !preg_match('/^@[a-zA-Z_]+(\(\'[^\']+\'\))?$/', $value)
                            || !preg_match('/^[0-9]+(\.[0-9]+)?$/', $value)
                        ) {
                            $value = ClickHouseConnector::escapeValue("String:{$value}");
                        }
                        return $value;
                    }, $values);
                    $whereRow[] = "`{$item['field']}` {$item['op']} (" . implode(',', $values) . ")";
                } else {
                    $value = trim($item['value']);
                    if (
                        !preg_match('/^@[a-zA-Z_]+(\(\'[^\']+\'\))?$/', $value)
                        && !preg_match('/^[0-9]+(\.[0-9]+)?$/', $value)
                    ) {
                        $value = ClickHouseConnector::escapeValue("String:{$value}");
                    }
                    $whereRow[] = "`{$item['field']}` {$item['op']} " . $value;
                }
            }
            if (!empty($whereRow)) {
                $where[] = $whereRow;
            }
        }
        $where = array_map(function ($row) {
            return "(" . implode(' OR ', $row) . ")";
        }, $where);

        $selectStmt = "SELECT " . implode(', ', $selects);
        $fromStmt = "FROM {$table}";
        $whereStmt = "";
        $groupByStmt = "";
        if ($where) {
            $whereStmt = "WHERE " . implode(' AND ', $where);
        }
        if (count($groups) != count($selects)) {
            $groupByStmt = "GROUP BY " . implode(',', $groups);
        }

        return "$selectStmt $fromStmt $whereStmt $groupByStmt";
    }
}
