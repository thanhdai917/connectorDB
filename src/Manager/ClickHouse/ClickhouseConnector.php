<?php


namespace Skeleton\ConnectorDb\Manager\ClickHouse;


use Illuminate\Support\Facades\Log;
use Skeleton\ConnectorDb\Manager\CSVParser\CSVParser;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\BooleanValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\DateTimeValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\DateValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\ExpressionValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\FloatValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\IntValue;
use Skeleton\ConnectorDb\Manager\ClickHouse\ParamValue\StringValue;
use Skeleton\ConnectorDb\Manager\SelectResult;
use Skeleton\ConnectorDb\Manager\ConnectionInformation;
use Skeleton\ConnectorDb\Manager\SelectStatement;
use GuzzleHttp\Client as HttpClient;
use GuzzleHttp\Exception\BadResponseException;
use GuzzleHttp\Exception\GuzzleException;
use Illuminate\Support\Facades\Http;
use Skeleton\ConnectorDb\Manager\Column;
use Skeleton\ConnectorDb\Manager\DBConnector;
use Skeleton\ConnectorDb\Manager\Utils\EmulateBindParam;

class ClickHouseConnector implements DBConnector {
    use EmulateBindParam;

    const DEFAULT_PORT = 8123;
    const DEFAULT_HOST = 'localhost';

    const TYPE_MAP = [
        'UInt8'       => Column::TYPE_NUMBER,
        'UInt16'      => Column::TYPE_NUMBER,
        'UInt32'      => Column::TYPE_NUMBER,
        'UInt64'      => Column::TYPE_NUMBER,
        'UInt256'     => Column::TYPE_NUMBER,
        'Int8'        => Column::TYPE_NUMBER,
        'Int16'       => Column::TYPE_NUMBER,
        'Int32'       => Column::TYPE_NUMBER,
        'Int64'       => Column::TYPE_NUMBER,
        'Int128'      => Column::TYPE_NUMBER,
        'Int256'      => Column::TYPE_NUMBER,
        'Float32'     => Column::TYPE_NUMBER,
        'Float64'     => Column::TYPE_NUMBER,
        'Decimal'     => Column::TYPE_NUMBER,
        'Decimal32'   => Column::TYPE_NUMBER,
        'Decimal64'   => Column::TYPE_NUMBER,
        'Decimal128'  => Column::TYPE_NUMBER,
        'Decimal256'  => Column::TYPE_NUMBER,
        'String'      => Column::TYPE_STRING,
        'Fixedstring' => Column::TYPE_STRING,
        'Date'        => Column::TYPE_DATE_TIME,
        'Datetime'    => Column::TYPE_DATE_TIME,
        'Datetime64'  => Column::TYPE_DATE_TIME,
    ];
    /**
     * @var array
     */
    private $config;

    /**
     * PostgresConnector constructor.
     *
     * @param string $connectionString
     *
     * @throws ClickHouseConnectorException
     */
    public function __construct(string $connectionString) {
        $this->connect($connectionString);
    }

    /**
     * @param string $connectionString
     *
     * @return DBConnector
     * @throws ClickHouseConnectorException
     */
    public function connect(string $connectionString): DBConnector {
        $config = json_decode($connectionString, TRUE);
        if ($config === NULL) {
            throw new ClickHouseConnectorException("Wrong json format");
        }
        try {
            if (empty($config['port'])) {
                $config['port'] = (string) self::DEFAULT_PORT;
            }
            if (empty($config['host'])) {
                $config['host'] = (string) self::DEFAULT_HOST;
            }
            $this->config = $config;
        } catch (\Exception $e) {
            throw new ClickHouseConnectorException(
                $e->getMessage(),
                $e->getCode(),
                $e->getPrevious()
            );
        }
        return $this;
    }

    public function select(string $sql, array $bindParams = []): SelectResult {
        $response = $this->sendQuery($sql, $bindParams);
        return new ClickHouseSelectResult(
            $response,
            $sql,
            $bindParams
        );
    }

    public function getInformation(): ConnectionInformation {
        $version = trim(stream_get_contents($this->sendQuery('SELECT version() as version', [], NULL)));
        return new ConnectionInformation(
            'clickhouse',
            $version
        );
    }

    public function selectStatement(string $sql, array $bindParams = []): SelectStatement {
        return new ClickHouseSelectStatement($this, $sql, $bindParams);
    }

    /**
     * @param string $column
     * @param string $period
     *
     * @return string
     * @throws ClickHouseConnectorException
     */
    public function groupTimestamp(string $column, string $period): string {
        $lastCharacter = substr($period, -1);
        $amount = (int) substr($period, 0, strlen($period) -1);
        if (!$amount) {
            throw new ClickHouseConnectorException(
                'Wrong period format'
            );
        }
        $map = [
            'm' => 'MINUTE',
            'h' => 'HOUR',
            'd' => 'DAY',
            'M' => 'MONTH',
            'y' => 'YEAR',
        ];
        if (!($type = $map[$lastCharacter])) {
            throw new ClickHouseConnectorException(
                'Wrong period format'
            );
        }
        return "toStartOfInterval(\"$column\",  INTERVAL $amount $type )";
    }

    /**
     * @param        $query
     * @param array  $params
     * @param string $format
     *
     * @return resource|null
     * @throws ClickHouseConnectorException
     */
    private function sendQuery($query, $params = [], $format = 'TabSeparatedWithNamesAndTypes') {
        $query = trim($query, " \t\n\r\0\x0B;");
        $query = self::emulateBindParam($query, $params);
        $host = urlencode($this->config['host']);
        $port = urlencode($this->config['port']);
        $database = urlencode($this->config['database']);
        $setting = [];
        $client = new HttpClient([
            'timeout' => config('dataflake.db_connector.clickhouse.timeout', 20),
            'headers' => [
                'X-ClickHouse-User' => $this->config['username'],
                'X-ClickHouse-Key'  => $this->config['password'],
                'X-ClickHouse-Format'  => $format ? $format : 'TabSeparated',
                'Content-Type' => 'text/plain'
            ]
        ]);
        $protocol = 'http';
        if (!empty($this->config['secure'])) {
            $protocol = 'https';
        }
        try {
            $response = $client->post("$protocol://{$host}:{$port}/?database={$database}", [
                'body' => $query,
            ]);
        } catch (BadResponseException $e) {
            throw new ClickHouseConnectorException((string) $e->getResponse()->getBody());
        } catch (GuzzleException $e) {
            throw new ClickHouseConnectorException($e->getMessage());
        }
        return $response->getBody()->detach();
    }

    public function putQueryToDatabase($sql) {
        $this->sendQuery($sql, $bindParams);
        return true;
    }
    /**
     * @param        $table
     * @param        $columns
     * @param string $tabSeparatedData
     *
     * @return resource|null
     * @throws ClickHouseConnectorException
     */
    private function bulkInsertQuery($table, $columns, $tabSeparatedData) {
        $columnNames = '`' . implode('`, `', $columns) . '`';
        $queryParams = [
            'database=' . urlencode($this->config['database']),
            'input_format_tsv_empty_as_default=1',
            'query=' . urlencode("INSERT INTO `$table` ($columnNames) FORMAT TabSeparated")
        ];
        $host = urlencode($this->config['host']);
        $port = urlencode($this->config['port']);
        $client = new HttpClient([
            'headers' => [
                'X-ClickHouse-User' => $this->config['username'],
                'X-ClickHouse-Key'  => $this->config['password'],
                'Content-Type' => 'text/plain'
            ]
        ]);
        $protocol = 'http';
        if (!empty($this->config['secure'])) {
            $protocol = 'https';
        }
        try {
            $response = $client->post("$protocol://{$host}:{$port}/?" . implode('&', $queryParams), [
                'body' => $tabSeparatedData,
            ]);
        } catch (BadResponseException $e) {
            throw new ClickHouseConnectorException((string) $e->getResponse()->getBody());
        } catch (GuzzleException $e) {
            throw new ClickHouseConnectorException($e->getMessage());
        }
        return $response->getBody()->detach();
    }

    public function hasTable(string $tableName): bool {
        try {
            $res = $this->select("SELECT 1 FROM `$tableName` LIMIT 1");
            return TRUE;
        } catch (\Exception $e) {
            Log::info($e);
            return FALSE;
        }
    }

    public function createTable(string $tableName, array $columns): bool {
        $sqlColumns = [];
        foreach ($columns as $column) {
            if (!empty($columnName = $column["name"])) {
                $sqlColumns[] = "`$columnName` " . self::convertToNativeType($column['type']);
            } else {
                throw new ClickHouseConnectorException("Wrong columns format: " . json_encode($columns));
            }
        }
        $sql = "CREATE TABLE `$tableName` (" . implode(',', $sqlColumns) . ") ENGINE = StripeLog";
        $result = $this->sendQuery($sql, [], '');
        return true;
    }

    public function dropTable(string $tableName): bool {
        $sql = "DROP TABLE `$tableName`";
        $result = $this->sendQuery($sql, [], '');
        return true;
    }

    public function import($table, CSVParser $parser, array $columns): int {
        $total = 0;
        $columnsName = array_map(function ($column) {
            return $column['name'];
        }, $columns);

        $columnsIndex = array_map(function ($column) {
            return $column['index'];
        }, $columns);

        while ($rows = $parser->fetchRows(1000, $columnsIndex)) {
            $total += count($rows);
            $tabSeparatedData = implode("\n", array_map(function ($row) {
                return implode("\t", array_map(function ($col) {
                    return str_replace([
                        "\t",
                        "\n"
                    ], [
                        '\t',
                        '\n'
                    ], trim($col));
                }, $row));
            }, $rows));
            $this->bulkInsertQuery($table, $columnsName, $tabSeparatedData);
        }
        return $total;
    }

    public function getTables(): array {
        $stream = $this->sendQuery("SHOW TABLES");
        $tables = [];
        $line = 0;
        while ($row = fgets($stream)) {
            $line ++;
            if ($line <= 2) {
                continue;
            }
            $tables[] = trim($row);
        }
        return $tables;
    }

    public function getTableDetail(string $table): array {
        $result = $this->select("DESCRIBE $table", compact('table'));
        return $result->getRows()->map(function ($row) {
            return [
                'name' => $row['name'],
                'type' => $row['type'],
            ];
        })->toArray();
    }

    static function convertToNativeType(string $type): string {
        switch (strtolower($type)) {
            case 'float':
                return 'DOUBLE';
            case 'integer':
                return 'BIGINT';
            case 'datetime':
                return 'DateTime(\'UTC\')';
            case 'time':
            case 'string':
            default:
                return 'String';
        }
    }

    static function convertToPHPType(string $nativeType): string {
        if (strpos($nativeType, 'Date') === 0) {
            return Column::TYPE_DATE_TIME;
        }
        if (!empty(self::TYPE_MAP[$nativeType])) {
            return self::TYPE_MAP[$nativeType];
        }
        return Column::TYPE_OTHERS;
    }

    /**
     * @param string|null $value
     * @return string
     */
    public static function escapeValue(string|null $value): string {
        if (is_null($value)) {
            return 'NULL';
        }
        list($type, $realValue) = parseBindParamValue($value);
        $paramValue = match ($type) {
            'Float' => new FloatValue($realValue),
            'Int' => new IntValue($realValue),
            'Boolean' => new BooleanValue($realValue),
            'Date' => new DateValue($realValue),
            'Datetime' => new DateTimeValue($realValue),
            'Expression' => new ExpressionValue($realValue),
            default => new StringValue($realValue),
        };
        return (string) $paramValue;
    }
}
