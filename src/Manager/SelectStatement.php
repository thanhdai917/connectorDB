<?php


namespace Skeleton\ConnectorDb\Manager;


use Illuminate\Support\Collection;

interface SelectStatement {
    public function execute(): SelectResult;
    public function limit($limit): SelectStatement;
    public function offset($offset): SelectStatement;
}
