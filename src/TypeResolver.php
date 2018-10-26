<?php

namespace Albe\PhpEventStore;

class TypeResolver implements TypeResolverInterface {

	public function getEventClass($eventType) {
		if (class_exists($eventType)) {
			return $eventType;
		}
		$normalizedEventType = str_replace('.', '\\', $eventType);
		if (class_exists($normalizedEventType)) {
			return $normalizedEventType;
		}
		if ($eventType === 'Event') {
			return 'ArrayObject';
		}
		return 'stdClass';
	}

	public function getEventType($event) {
		$type = 'Event';
		if (is_array($event) || $event instanceof \ArrayAccess)
		{
			if (isset($event['type'])) {
				$type = $event['type'];
			}
		}
		if ($event instanceof EventWithType)
		{
			$type = $event->getType();
		}
		if (is_object($event))
		{
			$type = get_class($event);
		}
		return $type;
	}
}
