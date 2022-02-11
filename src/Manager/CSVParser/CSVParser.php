<?php


namespace Skeleton\ConnectorDb\Manager\CSVParser;


class CSVParser {
    /**
     * @var string
     */
    private $separator;

    /**
     * @var string
     */
    private $hasHeader;

    /**
     * @var string
     */
    private $enclosure;

    /**
     * @var resource
     */
    private $stream;

    /**
     * @var string[]
     */
    private $fields;

    /**
     * CSVParser constructor.
     *
     * @param resource $file stream resource
     */
    public function __construct($file, $separator = NULL, $enclosure = NULL, $hasHeader = TRUE) {
        $stream = $file;
        if (!$enclosure) {
            $enclosure = '"';
        }
        if (!$separator) {
            $firstLine = fgets($stream);
            $separator = $this->detectSeparator($firstLine);
            rewind($stream);
        }
        $this->enclosure = $enclosure;
        $this->separator = $separator;
        $this->hasHeader = $hasHeader;
        $this->stream = $stream;

        $fields = fgetcsv($this->stream, 0, $this->separator, $this->enclosure);
        $this->fields = $fields;
        $this->resetPointer();
    }

    public function resetPointer() {
        rewind($this->stream);
        if ($this->hasHeader) {
            fgets($this->stream);
        }
    }

    /**
     * @return array|false
     */
    public function fetchRow($columnsIndex = NULL) {
        $row = fgetcsv(
            $this->stream,
            0,
            $this->separator,
            $this->enclosure
        );
        if (!$columnsIndex) {
            return $row;
        }
        $filteredRow = [];
        foreach ($columnsIndex as $index) {
            if (isset($row[$index])) {
                $filteredRow[] = $row[$index];
            } else {
                throw new CSVParserException(
                    "Wrong data format: " . PHP_EOL
                    . json_encode($columnsIndex)  . PHP_EOL
                    . json_encode($row)  . PHP_EOL
                );
            }
        }
        return $filteredRow;
    }

    /**
     * @param int $chunkSize
     *
     * @return array
     */
    public function fetchRows($chunkSize = 1000, $columnsIndex = NULL) {
        $rows = [];
        $count = 0;
        while (
            $count < $chunkSize
            && ($row = fgetcsv($this->stream, 0, $this->separator, $this->enclosure))
        ) {
            if (count($row) == 1 && !trim($row[0])) {
                continue;
            }
            if (!$columnsIndex) {
                $rows[] = $row;
                continue;
            }
            $filteredRow = [];
            foreach ($columnsIndex as $index) {
                if (isset($row[$index])) {
                    $filteredRow[] = $row[$index];
                } else {
                    throw new CSVParserException(
                        "Wrong data format: " . PHP_EOL
                        . json_encode($columnsIndex)  . PHP_EOL
                        . json_encode($row)  . PHP_EOL
                    );
                }
            }
            $rows[] = $filteredRow;
            $count++;
        }
        return $rows;
    }

    /**
     * @param string $firstLine
     *
     * @return string
     */
    public static function detectSeparator(string $firstLine): string {
        $delimiters = [",", ";", "\t", "|"];
        $detectedDelimiter = ",";
        $maxCount = 0;
        foreach ($delimiters as $delimiter) {
            if (
                strpos($firstLine, $delimiter) !== FALSE
                && ($count = count(explode($delimiter, $firstLine))) > $detectedDelimiter
            ) {
                $maxCount = $count;
                $detectedDelimiter = $delimiter;
            }
        }
        return $detectedDelimiter;
    }

    public function getFields() {
        return $this->fields;
    }

    public function getEnclosure() {
        return $this->enclosure;
    }

    public function getSeparator() {
        return $this->separator;
    }

    public function checkHasHeader() {
        return $this->hasHeader;
    }
}
