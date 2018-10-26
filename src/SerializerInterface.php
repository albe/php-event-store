<?php
namespace Albe\PhpEventStore;

interface SerializerInterface {

	/**
	 * @param object $object
	 * @return array
	 */
	public function serialize($object);

	/**
	 * @param array $data
	 * @param string $class
	 * @return object
	 */
	public function deserialize($data, $class);

}