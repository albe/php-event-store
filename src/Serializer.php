<?php

namespace Albe\PhpEventStore;

use Symfony\Component\Serializer\Serializer as SymfonySerializer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Normalizer\DateTimeNormalizer;

class Serializer implements SerializerInterface {
	/**
	 * @var SymfonySerializer
	 */
	private $serializer;

	public function __construct() {
		$this->serializer = new SymfonySerializer(array(new ObjectNormalizer(), new DateTimeNormalizer()));
	}

	public function serialize($object) {
		return $this->serializer->normalize($object);
	}

	public function deserialize($data, $class) {
		return $this->serializer->denormalize($data, $class);
	}
}