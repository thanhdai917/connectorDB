<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class StringValue extends ParamValue {
    public function toSQL(): string {
        return "'" . preg_replace("/([\\\\'])/", "\\\\$1", $this->value) . "'";
    }
}
