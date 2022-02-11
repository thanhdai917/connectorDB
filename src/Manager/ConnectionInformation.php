<?php


namespace Skeleton\ConnectorDb\Manager;


class ConnectionInformation {
    private $dialect;
    private $version;

    public function __construct(string $dialect, string $version) {
        $this->dialect = $dialect;
        $this->version = $version;
    }

    public function getDialect(): string {
        return $this->dialect;
    }

    public function getVersion(): string {
        return $this->version;
    }

    public function toArray() {
        return [
            'dialect' => $this->dialect,
            'version' => $this->version,
        ];
    }
}
