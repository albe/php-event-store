<?php

require_once './TestFrameworkInATweet.php';

require_once '../vendor/autoload.php';

use Albe\PhpEventStore\EventStore;
use Albe\PhpEventStore\StreamName;

$eventStore = new EventStore(
	new \Albe\PhpEventStore\Serializer(),
	new \Albe\PhpEventStore\TypeResolver()
);

$testStream = new StreamName($testStream);

it('Can create a new stream', function () use ($eventStore, $testStream) {
	$eventStore->createStream($testStream, array('createdBy' => $_SERVER['SCRIPT_NAME']));
	return $eventStore->getStreamMetadata($testStream)['createdBy'] === $_SERVER['SCRIPT_NAME'];
});

it('Returns 0 as version for empty stream', function () use ($eventStore, $testStream) {
	return $eventStore->getLastStreamVersion($testStream) === 0;
});

it('Returns -1 as version for non-existing stream', function () use ($eventStore) {
	return $eventStore->getLastStreamVersion(new StreamName('not-existing-stream-name')) === -1;
});

it('Does not return stream metadata for lastEvent', function () use ($eventStore, $testStream) {
	return $eventStore->getLastEvent($testStream) === null;
});

it('Can commit multiple events', function () use ($eventStore, $testStream) {
	class DomainEvent implements \JsonSerializable
	{
		public function __construct(array $payload)
		{
			foreach ($payload as $key => $value) {
				$this->{$key} = $value;
			}
			$this->type = __CLASS__;
		}

		public function jsonSerialize()
		{
			return get_object_vars($this);
		}
	}

	$eventStore->commit($testStream, array(new DomainEvent(array('foo' => 'bar')), new DomainEvent(array('foo' => 'baz'))));
	var_dump($eventStore->getLastEvent($testStream));
	return $eventStore->getLastEvent($testStream)['payload']['foo'] === 'baz';
});

