<?php
namespace Albe\PhpEventStore;

interface TypeResolverInterface {

	/**
	 * @param string $eventType The event type that should be converted to a fully qualified classname
	 * @return string The fully qualified classname of the class to use for deserializing the event
	 */
	public function getEventClass($eventType);

	/**
	 * @param object|array $event The event object to get the event type for
	 * @return string The event type that is stored for the given event
	 */
	public function getEventType($event);

}