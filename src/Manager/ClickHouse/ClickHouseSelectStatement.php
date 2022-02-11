<?php


namespace Skeleton\ConnectorDb\Manager\ClickHouse;


use Skeleton\ConnectorDb\Manager\Column;
use Skeleton\ConnectorDb\Manager\DBConnector;
use Skeleton\ConnectorDb\Manager\DBConnectorException;
use Skeleton\ConnectorDb\Manager\SelectResult;
use Skeleton\ConnectorDb\Manager\SelectStatement;
use ClickHouseDB\Statement;
use Illuminate\Support\Collection;
use ClickHouseDB\Exception\DatabaseException;
use Illuminate\Support\Facades\Log;

class ClickHouseSelectStatement implements SelectStatement {
    /**
     * @var ClickHouseConnector
     */
    private $_connection;

    /**
     * @var string
     */
    private $_query;

    /**
     * @var array
     */
    private $_bindParams;

    /**
     * @var integer
     */
    private $_limit;

    /**
     * @var integer
     */
    private $_offset;

    public function __construct(
        $connection,
        $query,
        $bindParams = [],
        $offset = 0,
        $limit = 100
    ) {
        $this->_connection = $connection;
        $this->_query = $query;
        $this->_bindParams = $bindParams;
        $this->_limit = $limit;
        $this->_offset = $offset;
    }

    public function execute(): SelectResult {
        $query = trim($this->_query, " \t\n\r\0\x0B;");
        if ($this->_limit) {
            $query .= ' LIMIT ';
            if ($this->_offset) {
                $query .= (integer) $this->_offset;
                $query .= ', ';
            }
            $query .= (integer) $this->_limit;
        }
        return $this->_connection->select($query, $this->_bindParams);
    }

    public function limit($limit): SelectStatement {
        $this->_limit = $limit;
        return $this;
    }

    public function offset($offset): SelectStatement {
        $this->_offset = $offset;
        return $this;
    }
}
