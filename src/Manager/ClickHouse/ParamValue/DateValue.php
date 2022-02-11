<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class DateValue extends ParamValue {
    public function toSQL(): string {
        $sanitizedValue = "'" . preg_replace("/([\\\\'])/", "\\\\$1", $this->value) . "'";
        return "toDateOrNull($sanitizedValue)";
    }
}
