<?php

use Skeleton\ConnectorDb\Manager\DBConnector;
use Skeleton\ConnectorDb\Manager\DBConnectorException;
use Skeleton\ConnectorDb\Manager\ClickHouse\ClickHouseConnector;

if (!function_exists('connectDatabase')) {
    /**
     * @param string $dialect
     * @param string $connectionString
     *
     * @return DBConnector
     * @throws DBConnectorException
     */
    function connectDatabase(string $dialect, string $connectionString): DBConnector {
        if (!empty($Driver = config("dataflake.drivers.$dialect"))) {
            return new $Driver($connectionString);
        }
        throw new DBConnectorException("\"$dialect\" is not supported");
    }
}
if (!function_exists('getBindParamsFromQuery')) {
    function getBindParamsFromQuery(string $query): array {
        preg_match_all('/(?:[^:]:|[^@]@)([a-zA-Z0-9_]+)/', $query, $matches);
        return array_map(function ($match) {
            return trim(trim($match), ':');
        }, $matches[1]);
    }
}

if (!function_exists('parseBindParamValue')) {
    function parseBindParamValue(string $value): array {
        $paramTypePattern = '/^(?:(String|Int|Float|Date|Datetime|Boolean|Expression)(\[\])?:)?(.+)$/';
        preg_match($paramTypePattern, $value, $matches);
        if (preg_match($paramTypePattern, $value, $matches)) {
            return [
                // type => String|Int|Float|Date|Datetime|Boolean|Expression
                $matches[1] ? $matches[1] : 'String',
                // value => string
                $matches[3],
                // is an array (true => yes, false => no)
                (bool) $matches[2],
            ];
        } else {
            return [
                'String',
                '',
                FALSE,
            ];
        }
    }
}

if (!function_exists('buildQuery')) {
    function buildQuery(string $dialect, string $query, string $type): string {
        if (
            $type === 'standard-builder' &&
            !empty($QueryBuilder = config("dataflake.query_builder.$dialect"))
        ) {
            $queryBuilder = new $QueryBuilder($query);
            return $queryBuilder->toString();
        }
        return $query;
    }
}
