<?php


namespace Skeleton\ConnectorDb\Manager;


use Skeleton\ConnectorDb\Manager\CSVParser\CSVParser;

interface DBConnector {
    function __construct(string $connectionString);

    /**
     * @param string $connectionString
     *
     * @return $this
     */
    public function connect(string $connectionString): DBConnector;

    /**
     * @param string $sql
     * @param array  $bindParams
     *
     * @return SelectResult
     */
    public function select(string $sql, array $bindParams = []): SelectResult;

    /**
     * @param string $sql
     * @param array  $bindParams
     *
     * @return SelectStatement
     */
    public function selectStatement(string $sql, array $bindParams = []): SelectStatement;

    /**
     * @param string $column
     * @param string $period with format: _m, _h, _d, _M, _y
     *
     * @return string
     */
    public function groupTimestamp(string $column, string $period): string;

    /**
     * @return ConnectionInformation
     */
    public function getInformation(): ConnectionInformation;

    /**
     * @param string $tableName
     *
     * @return bool
     */
    public function hasTable(string $tableName): bool;

    /**
     * @param string $tableName
     * @param array  $columns [name => string, type => string][]
     *
     * @return bool
     */
    public function createTable(string $tableName, array $columns): bool;

    /**
     * @param string $tableName
     *
     * @return bool
     */
    public function dropTable(string $tableName): bool;

    /**
     * @param           $table
     * @param CSVParser $parser
     *
     * @return int total rows import
     */
    public function import($table, CSVParser $parser, array $columns): int;

    public function getTables(): array;

    public function getTableDetail(string $table): array;

    static function convertToNativeType(string $type): string;

    static function convertToPHPType(string $nativeType): string;
}
