package com.webworkz.playground.pipeline;

public abstract class Event {
	private final EventType type;

	public Event(EventType type) {
		this.type = type;
	}

	public EventType getType() {
		return type;
	}

}
