<?php


namespace Skeleton\ConnectorDb\Manager\ClickHouse;


use Skeleton\ConnectorDb\Manager\Column;
use Skeleton\ConnectorDb\Manager\DBConnectorException;
use Skeleton\ConnectorDb\Manager\SelectResult;
use ClickHouseDB\Statement;
use Illuminate\Support\Collection;
use ClickHouseDB\Exception\DatabaseException;
use Illuminate\Support\Facades\Log;

class ClickHouseSelectResult implements SelectResult {
    const SPECIAL_CHARS = [
        '\N' => NULL,
    ];

    /**
     * @var resource stream resource
     */
    private $responseStream;
    /**
     * @var string
     */
    private $query;

    /**
     * @var array
     */
    private $bindParams;

    /**
     * @var array;
     */
    private $columns = [];

    /**
     * PostgresSelectResult constructor.
     *
     * @param resource $responseStream
     * @param string $query
     * @param array $bindParams
     */
    function __construct($responseStream, $query, array $bindParams) {
        $fields = fgetcsv($responseStream, 9999, "\t");
        $types = fgetcsv($responseStream, 9999, "\t");
        foreach ($fields as $index => $field) {
            $this->columns[] = [
                'name' => stripcslashes($field),
                'type' => stripcslashes($types[$index]),
            ];
        }
        $this->responseStream = $responseStream;
        $this->query = $query;
        $this->bindParams = $bindParams;
    }

    /**
     * @return Collection
     * @throws DBConnectorException
     */
    public function getColumns(): Collection {
        try {
            $columns = collect();
            foreach ($this->columns as $column) {
                $columns->push(
                    new Column(
                        $column['name'],
                        ClickHouseConnector::convertToPHPType($column['type'])
                    )
                );
            }
            return $columns;
        } catch (DatabaseException $exception) {
            Log::error($exception);
            throw new DBConnectorException($exception->getMessage());
        }
    }

    public function getRows(int $limit = NULL): Collection {
        if ($limit === NULL) {
            $limit = 9999;
        }
        $position = 1;
        $rows = collect();
        $specialCharacters = array_keys(self::SPECIAL_CHARS);
        while ($position <= $limit && ($rowData = fgetcsv($this->responseStream, 0, "\t"))) {
            $position++;
            $row = [];
            foreach ($rowData as $colIndex => $colData) {
                $row[$this->columns[$colIndex]['name']] = (
                in_array($colData, $specialCharacters) ? self::SPECIAL_CHARS[$colData] : stripcslashes($colData)
                );
            }
            $rows->push($row);
        }
        return $rows;
    }

    /**
     * @return string
     */
    public function getFinalQuery(): string {
        return $this->query;
    }
}
