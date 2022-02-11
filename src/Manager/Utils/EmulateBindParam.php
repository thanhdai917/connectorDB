<?php


namespace Skeleton\ConnectorDb\Manager\Utils;


use Skeleton\ConnectorDb\Manager\DBConnectorException;

trait EmulateBindParam {
    /**
     * Override this
     * @param string|null $value
     *
     * @return string
     */
    public static abstract function escapeValue(string|null $value): string;

    /**
     * @throws DBConnectorException
     */
    public static function emulateBindParam(string $query, array $bindParams = []): string {
        $pattern = '/(?<=[\s,!@#$%^&*()\/\\\;:\'\"]|^)(@|\$)([a-zA-Z0-9_]+)(\(\'([^\']*)\'\))?(?=[\s,!@#$%^&*()\/\\\;:\'\"]|$)/';
        while (preg_match($pattern, $query, $matchResult, PREG_OFFSET_CAPTURE)) {
            $match = $matchResult[0][0];
            $startPoint = $matchResult[0][1];
            $prefix = $matchResult[1][0];
            $paramName = $matchResult[2][0];
            $defaultValue = !empty($matchResult[4]) ? $matchResult[4][0] : NULL;
            $rawValue = $bindParams[$paramName] ?? $defaultValue;
            if (
                $prefix == '$'
                && ($value = parseBindParamValue($rawValue)[1])
                && !preg_match('/^[a-zA-Z0-9_]+$/', $value)
            ) {
                throw new DBConnectorException("unsafe bind params is not allow" . PHP_EOL . "{$paramName} = {$value} ");
            } else if ($prefix != '$') {
                $value = self::escapeValue($rawValue);
            }
            $query = substr_replace(
                $query,
                $value ?? 'NULL',
                $startPoint,
                strlen($match)
            );
        }
        return $query;
    }
}
