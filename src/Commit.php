<?php

namespace Albe\PhpEventStore;

class Commit implements \Iterator {
	private $streamId;
	private $streamVersion;
	private $commitIndex;
	private $commitSize;
	private $events;

	public function __construct($stream, $streamVersion, array $events, $metadata = array()) {
		if (count($events) === 0) {
			throw new \RuntimeException('List of events to commit may not be empty!');
		}
		$this->commitId = uuid();
		$this->committedAt = round(microtime(true) * 1000);
		foreach ($metadata as $key => $value) {
			$this->{$key} = $value;
		}
		$this->streamId = $stream;
		$this->streamVersion = $streamVersion;
		$this->events = $events;
		$this->commitIndex = 1;
		$this->commitSize = count($events);
	}

	public function current()
	{
		$eventData = get_class_vars($this);
		unset($eventData['events']);
		$eventData['streamVersion'] += $this->commitIndex;
		$eventData['payload'] = $this->events[$this->commitIndex];
		return new StoredEvent($eventData);
	}

	public function key()
	{
		return $this->commitIndex;
	}

	public function next()
	{
		$this->commitIndex++;
	}

	public function rewind()
	{
		$this->commitIndex = 1;
	}

	public function valid()
	{
		return $this->commitIndex <= $this->commitSize;
	}
}
