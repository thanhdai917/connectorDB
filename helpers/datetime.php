<?php

if (!function_exists('parseDateTimeFromExpression')) {
    /**
     * @param string $expression
     * @param  array $t.
     *
     */
    function parseDateTimeFromExpression(string $expression, string $t): Datetime {
        $pattern = '/(\- |\+)( *[0-9]+ *[a-z]+ *)([0-9]+ *[a-z]+ *)/';
        while(preg_match($pattern, $expression)) {
            $expression = preg_replace($pattern, '$1$2$1$3', $expression);
        }
        $expression = preg_replace_callback('/([0-9]+ *)(y|mo|d|w|h|m|s)([^a-zA-Z]+|$)/', function ($matches) {
            $fullName = match ($matches[2]) {
                'y' => 'year',
                'mo' => 'month',
                'd' => 'day',
                'w' => 'week',
                'h' => 'hour',
                'm' => 'minute',
                's' => 'second',
            };
            return "{$matches[1]}{$fullName}{$matches[3]}";
        }, $expression);
        $expression = preg_replace('/^ *t/', $t, $expression);
        $immutable = new DateTimeImmutable($expression);
        return DateTime::createFromImmutable($immutable);
    }
}
