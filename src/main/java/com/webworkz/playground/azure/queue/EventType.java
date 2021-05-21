package com.webworkz.playground.azure.queue;

/**
 * {@link EventType} enum has all the different event types. It also maintains
 * the queue where these events would be published and processed from.
 *
 */
public enum EventType {
	EVENT_A("queue-a"), EVENT_B("queue-b");

	private String queueName;

	private EventType(String queueName) {
		this.queueName = queueName;
	}

	public String getQueueName() {
		return queueName;
	}

}
