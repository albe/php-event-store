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
	public function __construct(SerializerInterface $serializer, TypeResolverInterface $typeResolver, $tableName = 'trackmyrace-pay-eventstore', $options = array())
	{
		$this->client = new DynamoDbClient(array_merge(array(
			'region'  => 'eu-central-1',
			'version' => '2012-08-10'
		), $options));
		$this->marshaler = new Marshaler();
		$this->tableName = $tableName;
		$this->commits = array();
		$this->retries = 5;	// Amount of retries on concurrency errors with $expectedVersion = VERSION_ANY

		$this->serializer = $serializer;
		$this->typeResolver = $typeResolver;
		// TODO: Check consistency by verifying last events commitSize === commitIndex
	}

	public function setup()
	{
		$params = array(
			"TableName" => $this->tableName,
			"AttributeDefinitions" => array(
				array(
					"AttributeName" => "streamId",
					"AttributeType" => "S"
				),
				array(
					"AttributeName" => "streamVersion",
					"AttributeType" => "N"
				),
				array(
					"AttributeName" => "committedAt",
					"AttributeType" => "N"
				),
				array(
					"AttributeName" => "eventType",
					"AttributeType" => "S"
				),
				array(
					"AttributeName" => "payload",
					"AttributeType" => "S"
				),
				array(
					"AttributeName" => "commitId",
					"AttributeType" => "S"
				),
				array(
					"AttributeName" => "commitSize",
					"AttributeType" => "N"
				),
				array(
					"AttributeName" => "commitIndex",
					"AttributeType" => "N"
				)
			),
			"KeySchema" => array(
				array(
					"AttributeName" => "streamId",
					"KeyType" => "HASH"
				),
				array(
					"AttributeName" => "streamVersion",
					"KeyType" => "RANGE"
				)
			),
			"ProvisionedThroughput" => array(
				"ReadCapacityUnits" => 5,
				"WriteCapacityUnits" => 5,
			),
			"StreamSpecification" => array(
				"StreamEnabled" => true,
				"StreamViewType" => "NEW_IMAGE"
			)
		);
		return $this->client->createTable($params);
	}

	/**
	 * @param StreamName $stream
	 * @param array $metadata Arbitrary stream metadata 
	 */
	public function createStream(StreamName $stream, $metadata = array())
	{
		unset($metadata['streamId']);
		unset($metadata['streamVersion']);
		$committedAt = round(microtime(true) * 1000);
		$params = array(
			"TableName" => $this->tableName,
			"Item" => $this->marshaler->marshalItem(array_merge(array(
				"streamId" => (string)$stream,
				"streamVersion" => 0,
				"committedAt" => $committedAt,
			), $metadata)),
			// This implicates that the combination of streamId+streamVersion doesn't exist
			"ConditionExpression" => "attribute_not_exists(streamId)"
		);

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
		$params = array(
			"TableName" => $this->tableName,
			"ConsistentRead" => true,
			"Key" => $this->marshaler->marshalItem(array(
				"streamId" => (string)$stream,
				"streamVersion" => 0
			))
		);
		$result = $this->client->getItem($params);
		return $this->marshaler->unmarshalItem($result['Item']);
	}

	/**
	 * @param StreamName $stream
	 * @return int
	 */
	public function getLastStreamVersion(StreamName $stream)
	{
		$params = array(
			"TableName" => $this->tableName,
			"Limit" => 1,
			"ScanIndexForward" => false,
			"ConsistentRead" => true,
			"ProjectionExpression" => "streamVersion",
			"KeyConditionExpression" => "streamId = :streamId",
			"ExpressionAttributeValues" => $this->marshaler->marshalItem(array(
				":streamId" => (string)$stream
			))
		);
		$result = $this->client->query($params)['Items'];
		if (count($result) < 1) {
			return -1;
		}
		return $this->marshaler->unmarshalItem($result[0])['streamVersion'];
	}

	public function getEventStream(StreamName $stream, $minVersion = 0, $maxVersion = -1)
	{
		$params = array(
			"TableName" => $this->tableName,
			//"IndexName" => "byEventType",
			"ConsistentRead" => true,
			"KeyConditionExpression" => "streamId = :streamId AND streamVersion > :minVersion",
		);
		if ($maxVersion >= 0) {
			$params["Limit"] = ($maxVersion - $minVersion) + 1;
		}
	}

	/**
	 * @param StreamName $stream
	 * @return array
	 */
	public function getLastEvent(StreamName $stream)
	{
		$params = array(
			"TableName" => $this->tableName,
			"Limit" => 1,
			"ScanIndexForward" => false,
			"ConsistentRead" => true,
			"KeyConditionExpression" => "streamId = :streamId",
			"ExpressionAttributeValues" => $this->marshaler->marshalItem(array(
				":streamId" => (string)$stream
			))
		);
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
	 * @param int $expectedVersion
	 * @param array $metadata
	 */
	public function commit(StreamName $stream, array $events, $expectedVersion = self::VERSION_ANY, $metadata = array())
	{
		$params = array(
			"TableName" => $this->tableName,
			"Item" => array(),
			// This implicates that the combination of streamId+streamVersion doesn't exist
			"ConditionExpression" => "attribute_not_exists(streamId)"
		);

		if (!is_int($expectedVersion)) {
			$expectedVersion = self::VERSION_ANY;
		}

		$streamVersion = $expectedVersion >= 0 ? $expectedVersion : $this->getLastStreamVersion($stream);

		unset($metadata['streamId']);
		unset($metadata['streamVersion']);
		unset($metadata['payload']);
		unset($metadata['commitId']);
		unset($metadata['commitSize']);
		unset($metadata['commitIndex']);

		$commitSize = count($events);
		$commitId = uuid(); // This only needs to be unique within the stream

		$this->startTransaction((string)$stream, $streamVersion, $commitId);
		for ($commitIndex = 1; $commitIndex <= $commitSize; $commitIndex++) {
			$committedAt = round(microtime(true) * 1000);
			$streamVersion++;
			$event = $events[$commitIndex - 1];
			$eventType = $this->typeResolver->getEventType($event);
			$payload = $this->serializer->serialize($event);
			$params['Item'] = $this->marshaler->marshalItem(array_merge(array(
				"streamId" => (string)$stream,
				"streamVersion" => $streamVersion,
				"eventType" => $eventType,
				"payload" => $payload,
				"commitId" => $commitId,
				"commitSize" => $commitSize,
				"commitIndex" => $commitIndex,
				"committedAt" => $committedAt,
			), $metadata));

			try {
				$this->client->putItem($params);
			} catch (\Aws\DynamoDb\Exception\DynamoDbException $e) {
				$this->rollBack($streamVersion);
				if ($e->getAwsErrorCode() !== 'ConditionalCheckFailedException') {
					throw $e;
				}

				if ($expectedVersion !== self::VERSION_ANY) {
					throw new OptimisticConcurrencyException('Stream "' . $stream . '" at version ' . $streamVersion . ' already exists.');
				}

				// Exponential backoff with jitter when another process added a commit beforehand
				$delay = \Aws\RetryMiddleware::exponentialDelay(count($this->commits));
				usleep($delay);
				$this->commit($stream, $events, $expectedVersion, $metadata);
				break;
			}
		}
		$this->commitTransaction();
	}

	/**
	 * @param string $stream
	 * @param int $streamVersion
	 * @param string $commitId
	 */
	private function startTransaction($stream, $streamVersion, $commitId)
	{
		if (count($this->commits) > $this->retries) {
			throw new \RuntimeException('The event store is overloaded and can not finish the commit.');
		}
		array_push($this->commits, array($stream, $streamVersion, $commitId));
	}

	/**
	 * @param int $until
	 */
	private function rollBack($until)
	{
		$lastCommit = array_pop($this->commits);
		if (!$lastCommit) {
			throw new \LogicException('Trying to roll back outside of a started transaction.');
		}

		list($stream, $streamVersion, $commitId) = $lastCommit;
		if ($streamVersion >= $until -1) {
			// Nothing to do. Transaction failed on first item.
			return;
		}

		while ($streamVersion < $until) {
			$params = array(
				"TableName" => $this->tableName,
				"Key" => $this->marshaler->marshalItem(array(
					"streamId" => $stream,
					"streamVersion" => $streamVersion
				)),
				"ConditionExpression" => "attribute_exists(commitId) AND commitId = :commitId",
				"ExpressionAttributeValues" => array(
					":commitId" => $commitId
				)
			);
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
	}

	/**
	 * This just removes the current transaction from the stack
	 */
	private function commitTransaction()
	{
		$lastCommit = array_pop($this->commits);
		if (!$lastCommit) {
			throw new \LogicException('Trying to commit outside of a started transaction.');
		}
	}
}
