<?php

namespace Albe\PhpEventStore;

class EventStream implements \Iterator {

	/**
	 * @var StreamName
	 */
	protected $streamName;

	/**
	 * @var \Iterator
	 */
	protected $iterator;

	/**
	 * @var bool
	 */
	protected $started = false;

	protected function __construct(StreamName $streamName, \Iterator $iterator) {
		$this->streamName = $streamName;
		$this->iterator = $iterator;
	}

	public static function fromIterator(StreamName $streamName, \Iterator $iterator) {
		return new static($streamName, $iterator);
	}

	public static function fromArray(StreamName $streamName, array $array) {
		return static::fromIterator($streamName, new \ArrayIterator($array));
	}

	/**
	 * @return StreamName
	 */
	public function getStreamName() {
		return $this->streamName;
	}

	/**
	 * @return array
	 */
	public function toArray() {
		$events = [];
		foreach ($this as $event) {
			$events[] = $event;
		}
		return $events;
	}

	/**
	 * @param \Closure $callback
	 */
	public function forAll(\Closure $callback) {
		/* @var $event StoredEvent */
		foreach ($this as $event) {
			$callback($event->getPayload());
		}
	}

	/**
	 * Return the current element
	 * @link https://php.net/manual/en/iterator.current.php
	 * @return StoredEvent
	 */
	public function current()
	{
		return $this->iterator->current();
	}

	/**
	 * Move forward to next element
	 * @link https://php.net/manual/en/iterator.next.php
	 * @return void
	 */
	public function next()
	{
		$this->iterator->next();
	}

	/**
	 * Return the key of the current element
	 * @link https://php.net/manual/en/iterator.key.php
	 * @return mixed scalar on success, or null on failure.
	 */
	public function key()
	{
		return $this->iterator->key();
	}

	/**
	 * Checks if current position is valid
	 * @link https://php.net/manual/en/iterator.valid.php
	 * @return boolean Returns true on success or false on failure.
	 */
	public function valid()
	{
		return $this->iterator->valid();
	}

	/**
	 * Rewind the Iterator to the first element
	 * @link https://php.net/manual/en/iterator.rewind.php
	 * @return void
	 */
	public function rewind()
	{
		if ($this->started) {
			throw new \RuntimeException('Can not rewind EventStream after first use!');
		}
		$this->started = true;
		$this->iterator->rewind();
	}
}