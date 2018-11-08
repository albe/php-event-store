<?php

namespace Albe\PhpEventStore;

class StreamVersion {
	const ANY = -2;
	const EMPTY = -1;
	const START = 0;

	public static function Any() {
		return new static(static::ANY);
	}

	public static function Empty() {
		return new static(static::EMPTY);
	}

	public static function Start() {
		return new static(static::START);
	}

	/**
	 * @var int
	 */
	private $version;

	/**
	 * StreamVersion constructor.
	 * @param int $version
	 */
	public function __construct($version)
	{
		if (!is_int($version)) {
			throw new \InvalidArgumentException('StreamVersion argument must be an integer.');
		}
		if ($version < self::ANY) {
			throw new \InvalidArgumentException('StreamVersion argument out of bounds.');
		}
		$this->version = $version;
	}

	/**
	 * @return string
	 */
	public function __toString()
	{
		return (string)$this->version;
	}

	/**
	 * @return int
	 */
	public function value() {
		return $this->version;
	}

	public function range(self $other) {
		return abs($this->version - $other->value()) + 1;
	}

	/**
	 * @return bool
	 */
	public function isAny() {
		return $this->version === self::ANY;
	}

	/**
	 * @return bool
	 */
	public function isEmpty() {
		return $this->version === self::EMPTY;
	}

	/**
	 * @param StreamVersion $other
	 * @return bool
	 */
	public function matches(StreamVersion $other) {
		if ($this->isAny() || $other->isAny()) {
			return true;
		}
		return $this->value() === $other->value();
	}
}