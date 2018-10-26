<?php

namespace Albe\PhpEventStore;

class StreamName {
	private $name;

	/**
	 * StreamName constructor.
	 * @param string $name
	 */
	public function __construct($name)
	{
		if (!is_string($name)) {
			throw new \InvalidArgumentException('StreamName must be a string.');
		}
		if ($name === '') {
			throw new \InvalidArgumentException('StreamName may not be empty.');
		}
		$this->name = $name;
	}

	public function __toString()
	{
		return $this->name;
	}
}