<?php

namespace Skeleton\ConnectorDb\Manager;

abstract class ParamValue {
    function __construct(protected string $value) {}

    public function __toString(): string {
        return $this->toSQL();
    }

    public abstract function toSQL(): string;
}
