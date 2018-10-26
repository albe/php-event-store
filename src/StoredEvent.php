<?php

namespace Albe\PhpEventStore;

class StoredEvent implements \JsonSerializable {
	protected $streamId;
	protected $streamVersion;
	protected $committedAt;
	protected $payload;
	protected $type;
	protected $commitId;
	protected $commitSize;
	protected $commitIndex;

	public function __construct(array $data, $type = 'array') {
		$this->committedAt = round(microtime(true) * 1000);
		foreach ($data as $key => $value) {
			$this->{$key} = $value;
		}
		$this->type = $type;
	}

	public static function fromClass($eventClass) {
		$type = new EventType($eventClass);
		$data = array();
		$payload = (array)$eventClass;
		return new static($data,(string)$type);
	}

	public function jsonSerialize() {
		return get_object_vars($this);
	}

	public function getType() {
		return $this->type;
	}

	public function getPayload() {
		return $this->payload;
	}
}
