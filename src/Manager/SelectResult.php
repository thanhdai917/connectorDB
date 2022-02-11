<?php


namespace Skeleton\ConnectorDb\Manager;


use Illuminate\Support\Collection;

interface SelectResult {
    /**
     * @return Column[]|Collection Collection of column
     */
    public function getColumns(): Collection;

    /**
     * @param int|null $limit
     *
     * @return Collection Collection of row
     */
    public function getRows(int $limit = NULL): Collection;

    /**
     * @return string
     */
    public function getFinalQuery(): string;
}
