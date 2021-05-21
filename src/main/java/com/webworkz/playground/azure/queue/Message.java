package com.webworkz.playground.azure.queue;

import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Verify;

/**
 * Represent a message and captures miscellaneous details.
 *
 */
public final class Message {
	private final String messageId;
	private final String messageText;
	private final EventType eventType;

	public Message(String messageText, EventType eventType) {
		this(UUID.randomUUID().toString(), messageText, eventType);

	}

	public Message(String messageId, String messageText, EventType eventType) {
		Verify.verify(StringUtils.isNotBlank(messageText), "Message Text in a message shouldn't be null or empty");
		Verify.verify(eventType != null, "Event Type for a message shouldn't be null");

		this.messageId = messageId;
		this.messageText = messageText;
		this.eventType = eventType;

	}

	public String getMessageId() {
		return messageId;
	}

	public String getMessageText() {
		return messageText;
	}

	public EventType getEventType() {
		return eventType;
	}

}
