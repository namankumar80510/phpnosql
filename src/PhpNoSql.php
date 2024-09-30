<?php

declare(strict_types=1);

namespace Dikki\PhpNoSql;

/**
 * Class phpDb
 * 
 * A powerful NoSQL database implementation in PHP, designed to be as feature-rich as SQLite.
 */
class PhpNoSql implements \ArrayAccess, \Countable, \IteratorAggregate
{
    /** @var string The path to the database directory */
    private string $dbPath;

    /** @var array The in-memory data store */
    private array $data = [];

    /** @var array Configuration options */
    private array $config;

    /** @var bool Flag to track if data has been modified */
    private bool $isDirty = false;

    /** @var array Cache for faster lookups */
    private array $cache = [];

    /** @var array Indexes for faster lookups */
    private array $indexes = [];

    /** @var resource|null File handle for transaction log */
    private $transactionLog = null;

    /**
     * phpDb constructor.
     *
     * @param string $db_name The name of the database
     * @param array $config Configuration options
     */
    public function __construct(private string $db_name, array $config = [])
    {
        $this->config = array_merge([
            'db_path' => [
                'development' => __DIR__ . '/db',
                'production' => __DIR__ . '/db',
            ],
            'auto_commit' => true,
            'cache_size' => 1000,
            'encryption_key' => null,
            'compression' => false,
            'required_fields' => [],
            'indexes' => [],
        ], $config);

        $environment = getenv('APP_ENV') ?: 'development';
        $this->dbPath = $this->config['db_path'][$environment] . '/' . $this->db_name;
        if (!file_exists($this->dbPath)) {
            mkdir($this->dbPath, 0777, true);
        }
        $this->load();
        $this->buildIndexes();
    }

    /**
     * Create a new record in the database.
     *
     * @param array $record The record to create
     * @return string The ID of the created record
     * @throws \Exception If the record couldn't be saved or validation fails
     */
    public function create(array $record): string
    {
        try {
            $this->validateRecord($record);
            $id = $this->generateUniqueId();
            $this->data[$id] = $record;
            $this->isDirty = true;
            $this->updateIndexes($id, $record);
            $this->updateCache($id, $record);

            if ($this->config['auto_commit']) {
                if (!$this->commit()) {
                    throw new \Exception("Failed to commit changes");
                }
            }

            return $id;
        } catch (\Exception $e) {
            error_log("Failed to create record: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Read records from the database.
     *
     * @param array $where Conditions to filter the records
     * @param array $options Additional options (e.g., limit, offset, order)
     * @return array The matching records
     */
    public function read(array $where = [], array $options = []): array
    {
        $result = $this->useIndexIfPossible($where);
        if ($result === null) {
            $result = array_filter($this->data, function ($item) use ($where) {
                return $this->matchesWhere($item, $where);
            });
        }

        if (isset($options['order'])) {
            $this->sortResults($result, $options['order']);
        }

        if (isset($options['limit'])) {
            $result = array_slice($result, $options['offset'] ?? 0, $options['limit']);
        }

        return array_values($result);
    }

    /**
     * Update records in the database.
     *
     * @param array $where Conditions to filter the records to update
     * @param array $newData The new data to apply
     * @return int The number of updated records
     * @throws \Exception If validation fails or commit fails
     */
    public function update(array $where, array $newData): int
    {
        try {
            $updated = 0;
            foreach ($this->data as $id => $item) {
                if ($this->matchesWhere($item, $where)) {
                    $updatedRecord = array_merge($item, $newData);
                    $this->validateRecord($updatedRecord);
                    $this->data[$id] = $updatedRecord;
                    $this->updateIndexes($id, $updatedRecord);
                    $this->updateCache($id, $updatedRecord);
                    $updated++;
                }
            }

            if ($updated > 0) {
                $this->isDirty = true;
                if ($this->config['auto_commit']) {
                    if (!$this->commit()) {
                        throw new \Exception("Failed to commit changes");
                    }
                }
            }

            return $updated;
        } catch (\Exception $e) {
            error_log("Failed to update records: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Delete records from the database.
     *
     * @param array $where Conditions to filter the records to delete
     * @return int The number of deleted records
     */
    public function delete(array $where = []): int
    {
        if (empty($where)) {
            $deleted = count($this->data);
            $this->data = [];
            array_map('unlink', glob("$this->dbPath/*.php"));
            $this->indexes = [];
            $this->cache = [];
        } else {
            $deleted = 0;
            foreach ($this->data as $id => $item) {
                if ($this->matchesWhere($item, $where)) {
                    unset($this->data[$id]);
                    unlink($this->getRecordPath($id));
                    $this->removeFromIndexes($id);
                    $this->removeFromCache($id);
                    $deleted++;
                }
            }
        }

        if ($deleted > 0) {
            $this->isDirty = true;
            if ($this->config['auto_commit']) {
                $this->commit();
            }
        }

        return $deleted;
    }

    /**
     * Perform a custom query on the database.
     *
     * @param callable $callback The query function
     * @return array The query results
     */
    public function query(callable $callback): array
    {
        return array_filter($this->data, $callback);
    }

    /**
     * Commit changes to the database.
     *
     * @return bool Whether the commit was successful
     */
    public function commit(): bool
    {
        if (!$this->isDirty) {
            return true;
        }

        $tempDir = $this->dbPath . '/temp_' . uniqid();
        mkdir($tempDir);

        try {
            $success = true;
            foreach ($this->data as $id => $record) {
                $tempPath = $tempDir . '/' . $id . '.php';
                if (!$this->saveRecord($id, $tempPath)) {
                    $success = false;
                    error_log("Failed to save record $id during commit");
                    break;
                }
            }

            if ($success) {
                $oldFiles = glob($this->dbPath . '/*.php');
                foreach ($oldFiles as $file) {
                    unlink($file);
                }
                $newFiles = glob($tempDir . '/*.php');
                foreach ($newFiles as $file) {
                    rename($file, $this->dbPath . '/' . basename($file));
                }
                $this->isDirty = false;
            }

            rmdir($tempDir);
            return $success;
        } catch (\Exception $e) {
            error_log("Commit failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Begin a transaction.
     */
    public function beginTransaction(): void
    {
        $this->config['auto_commit'] = false;
        $this->transactionLog = fopen($this->dbPath . '/transaction.log', 'w');
    }

    /**
     * End a transaction and commit changes.
     */
    public function endTransaction(): void
    {
        $this->commit();
        $this->config['auto_commit'] = true;
        if ($this->transactionLog) {
            fclose($this->transactionLog);
            unlink($this->dbPath . '/transaction.log');
            $this->transactionLog = null;
        }
    }

    /**
     * Roll back changes since the last commit.
     */
    public function rollback(): void
    {
        $this->load();
        $this->isDirty = false;
        if ($this->transactionLog) {
            fclose($this->transactionLog);
            unlink($this->dbPath . '/transaction.log');
            $this->transactionLog = null;
        }
    }

    /**
     * Get the number of records in the database.
     *
     * @return int The number of records
     */
    public function count(): int
    {
        return count($this->data);
    }

    /**
     * Check if a record exists.
     *
     * @param mixed $offset The record ID
     * @return bool Whether the record exists
     */
    public function offsetExists(mixed $offset): bool
    {
        return isset($this->data[$offset]);
    }

    /**
     * Get a record by ID.
     *
     * @param mixed $offset The record ID
     * @return mixed The record data
     */
    public function offsetGet(mixed $offset): mixed
    {
        return $this->data[$offset] ?? null;
    }

    /**
     * Set a record by ID.
     *
     * @param mixed $offset The record ID
     * @param mixed $value The record data
     */
    public function offsetSet(mixed $offset, mixed $value): void
    {
        if (is_null($offset)) {
            $this->create($value);
        } else {
            $this->update(['id' => $offset], $value);
        }
    }

    /**
     * Unset a record by ID.
     *
     * @param mixed $offset The record ID
     */
    public function offsetUnset(mixed $offset): void
    {
        $this->delete(['id' => $offset]);
    }

    /**
     * Get an iterator for the database records.
     *
     * @return \ArrayIterator An iterator for the records
     */
    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->data);
    }

    /**
     * Check if a record matches the given conditions.
     *
     * @param array $item The record to check
     * @param array $where The conditions to match
     * @return bool Whether the record matches
     */
    private function matchesWhere(array $item, array $where): bool
    {
        foreach ($where as $key => $value) {
            if (!isset($item[$key]) || $item[$key] !== $value) {
                return false;
            }
        }
        return true;
    }

    /**
     * Save a record to the database.
     *
     * @param string $id The record ID
     * @param string|null $customPath Custom path to save the record (for atomic transactions)
     * @return bool Whether the save was successful
     */
    private function saveRecord(string $id, ?string $customPath = null): bool
    {
        $filePath = $customPath ?? $this->getRecordPath($id);
        $content = '<?php return ' . var_export($this->data[$id], true) . ';';

        if ($this->config['compression']) {
            $content = gzcompress($content);
        }

        if ($this->config['encryption_key']) {
            $content = $this->encrypt($content, $this->config['encryption_key']);
        }

        $fp = fopen($filePath, 'c');
        if (flock($fp, LOCK_EX)) {
            $success = fwrite($fp, $content) !== false;
            fflush($fp);
            flock($fp, LOCK_UN);
            fclose($fp);
            return $success;
        }

        fclose($fp);
        return false;
    }

    /**
     * Load the database from disk.
     */
    private function load(): void
    {
        $files = glob("$this->dbPath/*.php");
        $this->data = [];
        $this->cache = [];
        foreach ($files as $file) {
            $id = basename($file, '.php');
            $content = file_get_contents($file);

            if ($this->config['encryption_key']) {
                $content = $this->decrypt($content, $this->config['encryption_key']);
            }

            if ($this->config['compression']) {
                $content = gzuncompress($content);
            }

            $this->data[$id] = include $file;
            $this->updateCache($id, $this->data[$id]);
        }
    }

    /**
     * Get the file path for a record.
     *
     * @param string $id The record ID
     * @return string The file path
     */
    private function getRecordPath(string $id): string
    {
        return $this->dbPath . '/' . $id . '.php';
    }

    /**
     * Generate a unique ID for a new record.
     *
     * @return string The unique ID
     */
    private function generateUniqueId(): string
    {
        return uniqid('', true);
    }

    /**
     * Sort the results based on the given order.
     *
     * @param array $results The results to sort
     * @param array $order The order specification
     */
    private function sortResults(array &$results, array $order): void
    {
        usort($results, function ($a, $b) use ($order) {
            foreach ($order as $key => $direction) {
                if (is_array($direction)) {
                    $subKey = key($direction);
                    $subDirection = current($direction);
                    $cmp = $this->compareValues($a[$key][$subKey], $b[$key][$subKey]);
                    if ($cmp !== 0) {
                        return $subDirection === 'DESC' ? -$cmp : $cmp;
                    }
                } else {
                    $cmp = $this->compareValues($a[$key], $b[$key]);
                    if ($cmp !== 0) {
                        return $direction === 'DESC' ? -$cmp : $cmp;
                    }
                }
            }
            return 0;
        });
    }

    /**
     * Compare two values for sorting.
     *
     * @param mixed $a First value
     * @param mixed $b Second value
     * @return int Comparison result
     */
    private function compareValues($a, $b): int
    {
        if (is_numeric($a) && is_numeric($b)) {
            return $a <=> $b;
        }
        return strcasecmp((string)$a, (string)$b);
    }

    /**
     * Encrypt data.
     *
     * @param string $data The data to encrypt
     * @param string $key The encryption key
     * @return string The encrypted data
     */
    private function encrypt(string $data, string $key): string
    {
        $ivlen = openssl_cipher_iv_length($cipher = "AES-256-CBC");
        $iv = openssl_random_pseudo_bytes($ivlen);
        $ciphertext_raw = openssl_encrypt($data, $cipher, $key, OPENSSL_RAW_DATA, $iv);
        $hmac = hash_hmac('sha256', $ciphertext_raw, $key, true);
        return base64_encode($iv . $hmac . $ciphertext_raw);
    }

    /**
     * Decrypt data.
     *
     * @param string $data The data to decrypt
     * @param string $key The decryption key
     * @return string The decrypted data
     */
    private function decrypt(string $data, string $key): string
    {
        $c = base64_decode($data);
        $ivlen = openssl_cipher_iv_length($cipher = "AES-256-CBC");
        $iv = substr($c, 0, $ivlen);
        $hmac = substr($c, $ivlen, $sha2len = 32);
        $ciphertext_raw = substr($c, $ivlen + $sha2len);
        $original_plaintext = openssl_decrypt($ciphertext_raw, $cipher, $key, OPENSSL_RAW_DATA, $iv);
        $calcmac = hash_hmac('sha256', $ciphertext_raw, $key, true);
        if (hash_equals($hmac, $calcmac)) {
            return $original_plaintext;
        }
        throw new \Exception("Decryption failed: HMAC mismatch");
    }

    /**
     * Validate the schema of a record.
     * 
     * @param array $record The record to validate.
     * @throws \Exception If validation fails.
     */
    private function validateRecord(array $record): void
    {
        $requiredFields = $this->config['required_fields'] ?? [];
        foreach ($requiredFields as $field) {
            if (!array_key_exists($field, $record)) {
                throw new \Exception("Missing required field: $field");
            }
        }
    }

    /**
     * Build indexes for faster lookups.
     */
    private function buildIndexes(): void
    {
        $this->indexes = [];
        foreach ($this->config['indexes'] as $field) {
            $this->indexes[$field] = [];
            foreach ($this->data as $id => $record) {
                if (isset($record[$field])) {
                    $this->indexes[$field][$record[$field]][] = $id;
                }
            }
        }
    }

    /**
     * Update indexes when a record is added or modified.
     *
     * @param string $id The record ID
     * @param array $record The record data
     */
    private function updateIndexes(string $id, array $record): void
    {
        foreach ($this->config['indexes'] as $field) {
            if (isset($record[$field])) {
                $this->indexes[$field][$record[$field]][] = $id;
            }
        }
    }

    /**
     * Remove a record from indexes when it's deleted.
     *
     * @param string $id The record ID
     */
    private function removeFromIndexes(string $id): void
    {
        foreach ($this->indexes as &$index) {
            foreach ($index as &$ids) {
                $ids = array_diff($ids, [$id]);
            }
        }
    }

    /**
     * Use index for faster lookups if possible.
     *
     * @param array $where The conditions to match
     * @return array|null The matching records or null if index can't be used
     */
    private function useIndexIfPossible(array $where): ?array
    {
        if (count($where) === 1) {
            $field = key($where);
            $value = current($where);
            if (isset($this->indexes[$field][$value])) {
                return array_intersect_key($this->data, array_flip($this->indexes[$field][$value]));
            }
        }
        return null;
    }

    /**
     * Update the cache when a record is added or modified.
     *
     * @param string $id The record ID
     * @param array $record The record data
     */
    private function updateCache(string $id, array $record): void
    {
        if (count($this->cache) >= $this->config['cache_size']) {
            array_shift($this->cache);
        }
        $this->cache[$id] = $record;
    }

    /**
     * Remove a record from the cache when it's deleted.
     *
     * @param string $id The record ID
     */
    private function removeFromCache(string $id): void
    {
        unset($this->cache[$id]);
    }

    /**
     * Backup the database.
     *
     * @param string $backupPath The path to store the backup
     * @return bool Whether the backup was successful
     */
    public function backup(string $backupPath): bool
    {
        try {
            $zip = new \ZipArchive();
            if ($zip->open($backupPath, \ZipArchive::CREATE) !== TRUE) {
                throw new \Exception("Cannot open $backupPath");
            }

            $files = glob($this->dbPath . '/*.php');
            foreach ($files as $file) {
                $zip->addFile($file, basename($file));
            }

            $zip->close();
            return true;
        } catch (\Exception $e) {
            error_log("Backup failed: " . $e->getMessage());
            return false;
        }
    }

    /**
     * Restore the database from a backup.
     *
     * @param string $backupPath The path of the backup file
     * @return bool Whether the restore was successful
     */
    public function restore(string $backupPath): bool
    {
        try {
            $zip = new \ZipArchive();
            if ($zip->open($backupPath) !== TRUE) {
                throw new \Exception("Cannot open $backupPath");
            }

            $zip->extractTo($this->dbPath);
            $zip->close();

            $this->load();
            $this->buildIndexes();
            return true;
        } catch (\Exception $e) {
            error_log("Restore failed: " . $e->getMessage());
            return false;
        }
    }
}
