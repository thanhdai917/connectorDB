<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class FloatValue extends ParamValue {
    public function toSQL(): string {
        return (string) (float) $this->value;
    }
}
