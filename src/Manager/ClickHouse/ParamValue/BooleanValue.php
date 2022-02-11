<?php

namespace Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue;

use Skeleton\ConnectorDb\Manager\ParamValue;

class BooleanValue extends ParamValue {
    public function toSQL(): string {
        $sanitizedValue = strtolower(trim($this->value));
        if (in_array($sanitizedValue, ['false', '0'])) {
            return 'FALSE';
        }
        return 'TRUE';
    }
}
