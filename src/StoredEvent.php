<?php

namespace Albe\PhpEventStore;

/*
 * Purpose? StoredEvent(EventInterface)->serialize->deserialize->
 */
class StoredEvent implements \JsonSerializable {
	protected $streamId;
	protected $streamVersion;
	protected $type;
	protected $payload;
	protected $metadata;
	protected $committedAt;
	protected $commitId;
	protected $commitIndex;

	public function __construct(array $data, $type = 'array') {
		$this->committedAt = round(microtime(true) * 1000);
		foreach ($data as $key => $value) {
			$this->{$key} = $value;
		}
		$this->type = $type;
	}

	public function jsonSerialize() {
		return get_object_vars($this);
	}

	public function getStreamName() {
		return $this->streamId;
	}

	public function getStreamVersion() {
		return $this->streamVersion;
	}

	public function getType() {
		return $this->type;
	}

	public function getPayload() {
		return $this->payload;
	}

	public function getMetadata() {
		return $this->metadata;
	}

	public function getCommitId() {
		return $this->commitId;
	}

	public function getCommitIndex() {
		return $this->commitIndex;
	}

	public function getCommittedAt() {
		return $this->committedAt;
	}

}
