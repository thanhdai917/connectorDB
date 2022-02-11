<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class ExpressionValue extends ParamValue {
    public function toSQL(): string {
        list($value, $bind) = array_pad(explode(',', strtolower($this->value)), 2, '');
        if(preg_match('/^ *t */', $value)) {
            $date = parseDateTimeFromExpression($value, $bind);
            return "'" . $date->format('Y-m-d H:i:s') . "'";
        }
        $sanitizedBind = "'" . preg_replace("/([\\\\'])/", "\\\\$1", $bind) . "'";
        $pattern = '/((^|[+-]) *?[a-z]+|((^|[+-]) *)?([0-9]+ *(hours?|minutes?|seconds?|days?|months?|years?|weeks?|d|mo|y|h|m|s|w) *)+)/';
        preg_match_all($pattern, $value, $matches);
        $parts = [];
        foreach ($matches[0] as $expression) {
            if ($expression === 'now') {
                $parts[] = 'now()';
            } else if ($expression === 't') {
                $parts[] = "toDateTimeOrNull($sanitizedBind)";
            } else if (preg_match_all('/([0-9]+) *(hours?|minutes?|seconds?|days?|months?|years?|weeks?|d|mo|y|h|m|s|w)/', $expression, $expressionMatches)) {
                $operator = str_starts_with(trim($expression), '-') ? '-' : '';
                for ($i = 0; $i < count($expressionMatches[0]); $i++) {
                    $value = $expressionMatches[1][$i];
                    $unit = $expressionMatches[2][$i];
                    $function = match ($unit) {
                        'hours', 'hour', 'h' => 'toIntervalHour',
                        'minutes', 'minute', 'm' => 'toIntervalMinute',
                        'seconds', 'second', 's' => 'toIntervalSecond',
                        'weeks', 'week', 'w' => 'toIntervalWeek',
                        'days', 'day', 'd' => 'toIntervalDay',
                        'months', 'month', 'mo' => 'toIntervalMonth',
                        'years', 'year', 'y' => 'toIntervalYear',
                    };
                    if (!empty($function)) {
                        $parts[] = "{$function}({$operator}{$value})";
                    }
                }
            }
        }
        if (!count($parts)) {
            return 'NULL';
        }
        return '(' . trim(implode(' + ', $parts)) . ')';
    }
}
