<?php

namespace Albe\PhpEventStore;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;

require_once '../vendor/autoload.php';

/**
 * Generate a random UUID V4 string.
 *
 * One of following functions needs to be available
 * in order to generate random bytes used for UUID:
 *  - random_bytes (requires PHP 7.0 or above) 
 *  - openssl_random_pseudo_bytes (requires 'openssl' module enabled)
 *  - mcrypt_create_iv (requires 'mcrypt' module enabled)
 *
 * More information about UUID v4, see:
 * https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29
 * https://tools.ietf.org/html/rfc4122#page-14
 * @return string
 */
function uuid() {
    if (function_exists("random_bytes")) {
        $bytes = random_bytes(16);
    } elseif (function_exists("openssl_random_pseudo_bytes")) {
        $bytes = openssl_random_pseudo_bytes(16);
    } elseif (function_exists("mcrypt_create_iv")) {
        $bytes = mcrypt_create_iv(16);
    } else {
        throw new \RuntimeException("No cryptographically secure random function available");
    }
	// set version to 0100
	$bytes[6] = chr(ord($bytes[6]) & 0x0f | 0x40);
	// set bits 6-7 to 10
	$bytes[8] = chr(ord($bytes[8]) & 0x3f | 0x80);
	return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
}

interface EventWithType {
	public function getType();
}

class EventType {

	public function __construct($event)
	{
		$this->type = 'Event';
		if (is_array($event) || $event instanceof \ArrayAccess)
		{
			if (isset($event['type'])) {
				$this->type = $event['type'];
			}
		}
		if ($event instanceof EventWithType)
		{
			$this->type = $event->getType();
		}
		if (is_object($event))
		{
			$this->type = get_class($event);
		}
	}

	public function __toString()
	{
		return $this->type;
	}
}

abstract class TransactionStatus {
	const OK = 0;
	const ACTIVE = 1;
	const COMMITTED = 2;
	const ROLLEDBACK = 3;
}

class Transaction {
	// Timeout in ms after which a transaction is considered aborted
	const COMMIT_TIMEOUT = 10 * 1000;

	protected $streamName;
	protected $commitId;
	protected $commitSize;
	protected $commitTs;
	protected $lastCommitVersion;
	protected $status;

	protected function __construct($streamName, $commitId, $commitSize, $commitTs, $lastCommitVersion) {
		$this->streamName = $streamName;
		$this->commitId = $commitId;
		$this->commitSize = $commitSize;
		$this->commitTs = $commitTs;
		$this->lastCommitVersion = $lastCommitVersion;
	}

	/**
	 * @param array $metadata
	 * @return Transaction
	 */
	public static function fromStreamMetadata(array $metadata) {
		return new static($metadata['streamId'], $metadata['commitId'], $metadata['commitSize'], $metadata['commitTs'], $metadata['commitVersion']);
	}

	/**
	 * @param string $streamName
	 * @param int $commitSize
	 * @param int $streamVersion
	 * @return Transaction
	 */
	public static function start($streamName, $commitSize, $streamVersion) {
		$commitId = uuid();
		$commitTs = static::getTimestamp();
		return new static($streamName, $commitId, $commitSize, $commitTs, $streamVersion);
	}

	public static function getTimestamp() {
		return (int)round(microtime(true) * 1000);
	}

	/**
	 * @return bool
	 */
	public function isTimedOut() {
		$now = static::getTimestamp();
		return ($now - $this->commitTs) > self::COMMIT_TIMEOUT;
	}

	public function getStreamName() {
		return $this->streamName;
	}

	public function getCommitId() {
		return $this->commitId;
	}

	public function getCommitSize() {
		return $this->commitSize;
	}

	public function getCommitTs() {
		return $this->commitTs;
	}

	public function getLastCommitVersion() {
		return $this->lastCommitVersion;
	}

	public function getCommitVersion() {
		return $this->lastCommitVersion + $this->commitSize;
	}
}

/*
Usage:
  Serializer::serialize(EventInterface $event): array
  Serializer::deserialize(array $data, string $className): EventInterface
  EventTypeResolver::getEventType(string $className): string
  EventTypeResolver::getClassName(string $eventType): string
  EventStore::__construct(Serializer $serializer, EventTypeResolver $eventTypeResolver)
  EventStore::commit(string $stream, EventInterface[] $events, int $expectedVersion)
  EventStore::getStream(string $stream, int $minVersion, int $maxVersion): EventStream
  EventStream::iterate(): EventInterface
  EventStream::forEach(): StoredEvent
  EventStream::getStream(): string
*/


class EventStore {

	const VERSION_ANY = -1;
	const VERSION_EMPTY = 0;

	/**
	 * @var DynamoDbClient
	 */
	protected $client;

	/**
	 * @var Marshaler
	 */
	protected $marshaler;

	/**
	 * @var SerializerInterface
	 */
	protected $serializer;

	/**
	 * @var TypeResolverInterface
	 */
	protected $typeResolver;

	/**
	 * @var array
	 */
	protected $commits;

	/**
	 * EventStore constructor.
	 * @param SerializerInterface $serializer
	 * @param TypeResolverInterface $typeResolver
	 * @param string $tableName
	 * @param array $options
	 */
	public function __construct(SerializerInterface $serializer, TypeResolverInterface $typeResolver, $tableName = 'trackmyrace-pay-eventstore', $options = [])
	{
		$this->client = new DynamoDbClient(array_merge([
			'region'  => 'eu-central-1',
			'version' => '2012-08-10'
		], $options));
		$this->marshaler = new Marshaler();
		$this->tableName = $tableName;
		$this->commits = [];
		$this->retries = 5;	// Amount of retries on concurrency errors with $expectedVersion = VERSION_ANY

		$this->serializer = $serializer;
		$this->typeResolver = $typeResolver;
		// TODO: Check consistency by verifying last events commitSize === commitIndex
	}

	public function setup()
	{
		$schema = [
			"AttributeDefinitions" => [
				[
					"AttributeName" => "streamId",
					"AttributeType" => "S"
				],
				[
					"AttributeName" => "streamVersion",
					"AttributeType" => "N"
				],
				[
					"AttributeName" => "committedAt",
					"AttributeType" => "N"
				],
				[
					"AttributeName" => "eventType",
					"AttributeType" => "S"
				],
				[
					"AttributeName" => "payload",
					"AttributeType" => "S"
				],
				[
					"AttributeName" => "commitId",
					"AttributeType" => "S"
				],
				[
					"AttributeName" => "commitSize",
					"AttributeType" => "N"
				],
				[
					"AttributeName" => "commitIndex",
					"AttributeType" => "N"
				]
			],
			"KeySchema" => [
				[
					"AttributeName" => "streamId",
					"KeyType" => "HASH"
				],
				[
					"AttributeName" => "streamVersion",
					"KeyType" => "RANGE"
				]
			],
			"StreamSpecification" => [
				"StreamEnabled" => true,
				"StreamViewType" => "NEW_IMAGE"
			],
		];

		try {
			$result = $this->marshaler->unmarshalItem($this->client->describeTable([
				"TableName" => $this->tableName
			])->toArray());
			$currentSchema = array_filter($result['Table'], function($k) {
				return in_array($k, ['AttributeDefinitions', 'KeySchema', 'StreamSpecification']);
			}, ARRAY_FILTER_USE_KEY);
			// TODO: Diff schemas and throw Exception if they differ
			return true;
		} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
			// if this exception is thrown, the table doesn't exist
			if ($e->getAwsErrorCode() !== 'ResourceNotFoundException') {
				throw $e;
			}
		}

		$params = array_merge([
			"TableName" => $this->tableName,
			"ProvisionedThroughput" => [
				"ReadCapacityUnits" => 5,
				"WriteCapacityUnits" => 5,
			],
		], $schema);
		return $this->client->createTable($params);
	}

	/**
	 * @param StreamName $stream
	 * @param array $metadata Arbitrary stream metadata 
	 */
	public function createStream(StreamName $stream, $metadata = [])
	{
		unset($metadata['streamId']);
		unset($metadata['streamVersion']);
		$committedAt = round(microtime(true) * 1000);
		$params = [
			"TableName" => $this->tableName,
			"Item" => $this->marshaler->marshalItem(array_merge([
				"streamId" => (string)$stream,
				"streamVersion" => 0,
				"committedAt" => $committedAt,
			], $metadata)),
			// This implicates that the combination of streamId+streamVersion doesn't exist
			"ConditionExpression" => "attribute_not_exists(streamId)"
		];

		try {
			$this->client->putItem($params);
		} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
			if ($e->getAwsErrorCode() !== 'ConditionalCheckFailedException') {
				throw $e;
			}
		}
	}

	/**
	 * @param StreamName $stream
	 */
	public function getStreamMetadata(StreamName $stream)
	{
		$params = [
			"TableName" => $this->tableName,
			"ConsistentRead" => true,
			"Key" => $this->marshaler->marshalItem([
				"streamId" => (string)$stream,
				"streamVersion" => 0
			])
		];
		$result = $this->client->getItem($params);
		return $this->marshaler->unmarshalItem($result['Item']);
	}

	/**
	 * @param StreamName $stream
	 * @return StreamVersion
	 */
	public function getLastStreamVersion(StreamName $stream)
	{
		$params = [
			"TableName" => $this->tableName,
			"Limit" => 1,
			"ScanIndexForward" => false,
			"ConsistentRead" => true,
			"ProjectionExpression" => "streamVersion",
			"KeyConditionExpression" => "streamId = :streamId",
			"ExpressionAttributeValues" => $this->marshaler->marshalItem([
				":streamId" => (string)$stream
			])
		];
		$result = $this->client->query($params)['Items'];
		if (count($result) < 1) {
			return StreamVersion::Empty();
		}
		return new StreamVersion($this->marshaler->unmarshalItem($result[0])['streamVersion']);
	}

	public function getEventStream(StreamName $stream, StreamVersion $minVersion = null, StreamVersion $maxVersion = null)
	{
		if ($minVersion === null) {
			$minVersion = StreamVersion::Start();
		}
		$params = [
			"TableName" => $this->tableName,
			"ConsistentRead" => true,
			"KeyConditionExpression" => "streamId = :streamId AND streamVersion > :minVersion",
			"ExpressionAttributeValues" => $this->marshaler->marshalItem([
				":streamId" => (string)$stream,
				":minVersion" => (int)$minVersion->value()
			])
		];
		if ($maxVersion !== null) {
			$params["Limit"] = $maxVersion->range($minVersion);
		}

		$generator = function($params) {
			while (true) {
				$result = $this->client->query($params);
				if (count($result['Items']) < 1) {
					return;
				}
				$events = $this->marshaler->unmarshalItem($result['Items']);
				foreach ($events as $event) {
					$eventClass = $this->typeResolver->getEventClass($event['eventType']);
					$event['payload'] = $this->serializer->deserialize($event['payload'], $eventClass);
					yield new StoredEvent($event);
				}
				if (!isset($result['LastEvaluatedKey'])) {
					return;
				}
				$params['ExclusiveStartKey'] = $result['LastEvaluatedKey'];
			}
		};
		return EventStream::fromIterator($stream, $generator($params));
/*
		$deserializer = function($event) use ($serializer, $typeResolver) {
			$eventClass = $this->typeResolver->getEventClass($event['eventType']);
			$event['payload'] = $this->serializer->deserialize($event['payload'], $eventClass);
			return new StoredEvent($event);
		};
		$iterator = new QueryBatchIterator($this->client, $params, $deserializer);
		return EventStream::fromIterator($iterator);
*/
	}

	/**
	 * @param StreamName $stream
	 * @return StoredEvent
	 */
	public function getLastEvent(StreamName $stream)
	{
		$params = [
			"TableName" => $this->tableName,
			"Limit" => 1,
			"ScanIndexForward" => false,
			"ConsistentRead" => true,
			"KeyConditionExpression" => "streamId = :streamId",
			"ExpressionAttributeValues" => $this->marshaler->marshalItem([
				":streamId" => (string)$stream
			])
		];
		$result = $this->client->query($params)['Items'];
		if (count($result) < 1) {
			return null;
		}
		$event = $this->marshaler->unmarshalItem($result[0]);
		if ($event['streamVersion'] === 0) {
			return null;
		}
		$eventClass = $this->typeResolver->getEventClass($event['eventType']);
		$event['payload'] = $this->serializer->deserialize($event['payload'], $eventClass);
		return new StoredEvent($event);
	}

	/**
	 * @param StreamName $stream
	 * @param array $events
	 * @param StreamVersion $expectedVersion
	 * @param array $metadata
	 */
	public function commit(StreamName $stream, array $events, StreamVersion $expectedVersion = null, $metadata = [])
	{
		$params = [
			"TableName" => $this->tableName,
			"Item" => [],
			// This implicates that the combination of streamId+streamVersion doesn't exist
			"ConditionExpression" => "attribute_not_exists(streamId)"
		];

		if ($expectedVersion === null) {
			$expectedVersion = StreamVersion::Any();
		}

		if ($expectedVersion->isEmpty()) {
			$streamVersion = 0;
		} elseif ($expectedVersion->isAny()) {
			$streamVersion = $this->getLastStreamVersion($stream);
		} else {
			$streamVersion = $expectedVersion->value();
		}

		// Avoid overwriting important generated event metadata
		unset($metadata['streamId']);
		unset($metadata['streamVersion']);
		unset($metadata['payload']);
		unset($metadata['commitId']);
		unset($metadata['commitSize']);
		unset($metadata['commitIndex']);

		$commitSize = count($events);
		$transaction = Transaction::start((string)$stream, count($events), $streamVersion);
		$result = $this->startTransaction($transaction);
		if ($result === false) {
			// Transaction failed because another transaction is active. Retry later.
			if ($this->commitAttempts >= Transaction::MAX_RETRIES) {
				$status = $this->checkTransactionStatus($stream);
			}
			$delay = \Aws\RetryMiddleware::exponentialDelay($this->commitAttempts);
			usleep($delay);
			$this->commit($stream, $events, $expectedVersion, $metadata);
			return;
		}
		for ($commitIndex = 1; $commitIndex <= $commitSize; $commitIndex++) {
			$committedAt = Transaction::getTimestamp();
			$streamVersion++;
			$event = $events[$commitIndex - 1];
			$eventType = $this->typeResolver->getEventType($event);
			$payload = $this->serializer->serialize($event);
			$params['Item'] = $this->marshaler->marshalItem([
				"streamId" => (string)$stream,
				"streamVersion" => $streamVersion,
				"eventType" => $eventType,
				"payload" => $payload,
				"metadata" => $metadata,
				"commitId" => $transaction->getCommitId(),
				//"commitSize" => $commitSize,
				"commitIndex" => $commitIndex,
				"committedAt" => $committedAt,
			]);

			try {
				$this->client->putItem($params);
			} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
				$this->rollBack($transaction);
				if ($e->getAwsErrorCode() !== 'ConditionalCheckFailedException') {
					throw $e;
				}

				if (!$expectedVersion->isAny()) {
					throw new OptimisticConcurrencyException('Stream "' . $stream . '" at version ' . $streamVersion . ' already exists.');
				}

				// Exponential backoff with jitter when another process added a commit beforehand
				$delay = \Aws\RetryMiddleware::exponentialDelay($this->commitAttempts);
				usleep($delay);
				$this->commit($stream, $events, $expectedVersion, $metadata);
				return;
			}
		}
		$this->commitTransaction($transaction);
	}

	private function updateStreamMetadata($stream, $updateExpression, $values = [], $conditionExpression = null)
	{
		$params = [
			"TableName" => $this->tableName,
			"Key" => $this->marshaler->marshalItem([
				"streamId" => (string)$stream,
				"streamVersion" => 0
			]),
			"ExpressionAttributeValues" => $this->marshaler->marshalItem($values),
			"UpdateExpression" => $updateExpression
		];
		if ($conditionExpression !== null) {
			$params['ConditionExpression'] = $conditionExpression;
		}

		try {
			$this->client->updateItem($params);
			return true;
		} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
			if ($e->getAwsErrorCode() !== 'ConditionalCheckFailedException') {
				throw $e;
			}
			return false;
		}
	}

	private function startTransaction(Transaction $tx)
	{
		$this->commitAttempts++;
		if ($tx->getCommitSize() === 1) {
			// A single write is always transactional, so nothing to do here
			return true;
		}
		$result = $this->updateStreamMetadata($tx->getStreamName(),
			"SET commitId = :commitId, commitSize = :commitSize, commitTs = :commitTs",
			[
				":lastCommitVersion" => $tx->getLastCommitVersion(),
				":commitId" => $tx->getCommitId(),
				":commitSize" => $tx->getCommitSize(),
				":commitTs" => $tx->getCommitTs()
			],
			"attribute_not_exists(commitId) AND commitVersion = :lastCommitVersion"
			);
		if ($result === false) {
			// Another transaction is currently running... retry later please!
			return false;
		}
	}

	private function rollBack(Transaction $tx)
	{
		$streamVersion = $tx->getLastCommitVersion() + 1;
		// DO THE ROLLBACK -> delete all items of the rolled back transaction
		while ($streamVersion < $tx->getCommitVersion()) {
			$params = [
				"TableName" => $this->tableName,
				"Key" => $this->marshaler->marshalItem([
					"streamId" => $tx->getStreamName(),
					"streamVersion" => $streamVersion
				]),
				"ConditionExpression" => "attribute_exists(commitId) AND commitId = :commitId",
				"ExpressionAttributeValues" => [
					":commitId" => $tx->getCommitId()
				]
			];
			try {
				$this->client->deleteItem($params);
				$streamVersion++;
			} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
				if ($e->getAwsErrorCode() === 'ConditionalCheckFailedException') {
					// Another commit has replaced the streamVersions in question, so nothing to do any more
					return;
				}

				// Retry with exponential backoff is already applied by the SDK, so if something still fails here, then things are really off
				throw $e;
			}
		}

		// Mark the transaction rolled back
		$result = $this->updateStreamMetadata($tx->getStreamName(),
			"REMOVE commitId",
			[
				":commitId" => $tx->getCommitId()
			],
			"attribute_exists(commitId) AND commitId = :commitId"
		);
		if ($result === false) {
			// The transaction is already rolled back or committed. Nothing to do here.
			return;
		}
	}

	private function commitTransaction(Transaction $transaction)
	{
		$this->commitAttempts = 0;
		$result = $this->updateStreamMetadata($transaction->getStreamName(),
			"REMOVE commitId SET commitVersion = :commitVersion",
			[
				":commitVersion" => $transaction->getCommitVersion(),
				":commitId" => $transaction->getCommitId()
			],
			"commitId = :commitId"
		);
		if ($result === false) {
			// The commit failed, because the transaction was aborted beforehand.
			return false;
		}
		return true;
	}

	private function checkTransactionStatus($stream)
	{
		$streamMetadata = $this->getStreamMetadata($stream);
		if (isset($streamMetadata['commitId'])) {
			// A transaction is running...
			$transaction = Transaction::fromStreamMetadata($streamMetadata);
			if ($transaction->isTimedOut()) {
				$lastEvent = $this->getLastEvent($stream);
				if ($lastEvent->getCommitIndex() === $transaction->getCommitSize()) {
					// The previous transaction was fully written, but not committed. Commit it now.
					$this->commitTransaction($transaction);
					return TransactionStatus::COMMITTED;
				}
				// The transaction timed out unfinished. We need to roll it back!
				$this->rollBack($transaction);
				return TransactionStatus::ROLLEDBACK;
			}
			return TransactionStatus::ACTIVE;
		}
		return TransactionStatus::OK;
	}
}
