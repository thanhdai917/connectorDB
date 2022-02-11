<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class IntValue extends ParamValue {
    public function toSQL(): string {
        return (string) (int) $this->value;
    }
}
