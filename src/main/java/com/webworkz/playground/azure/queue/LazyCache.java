package com.webworkz.playground.azure.queue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.microsoft.azure.storage.queue.MessagesURL;
import com.microsoft.azure.storage.queue.QueueURL;
import com.microsoft.azure.storage.queue.ServiceURL;
import com.microsoft.azure.storage.queue.models.DequeuedMessageItem;

import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

/**
 * {@link LazyCache} lazily constructs and caches objects
 *
 */
public class LazyCache {
	private final ServiceURL serviceURL;

	private final Map<String, QueueURL> queueURLsRegistry;
	private final Map<String, MessagesURL> messagesURLsRegistry;
	private final Map<String, Subject<DequeuedMessageItem>> pollingSubjectsRegistry;

	public LazyCache(ServiceURL serviceURL) {
		this.serviceURL = serviceURL;
		queueURLsRegistry = new ConcurrentHashMap<>();
		messagesURLsRegistry = new ConcurrentHashMap<>();
		pollingSubjectsRegistry = new ConcurrentHashMap<>();
	}

	private synchronized <T> T computeAndCache(String queueName, Map<String, T> registry,
			Function<String, T> computeFunction) {
		T t = computeFunction.apply(queueName);
		registry.put(queueName, t);
		return t;
	}

	public QueueURL getOrCreateQueueURL(String queueName) {
		final QueueURL result = queueURLsRegistry.get(queueName);
		return result == null ? computeAndCache(queueName, queueURLsRegistry, s -> serviceURL.createQueueUrl(s))
				: result;
	}

	public MessagesURL getOrCreateMessagesURL(String queueName) {
		final MessagesURL result = messagesURLsRegistry.get(queueName);
		return result == null
				? computeAndCache(queueName, messagesURLsRegistry, s -> getOrCreateQueueURL(s).createMessagesUrl())
				: result;
	}

	public Subject<DequeuedMessageItem> getOrCreatePollingSubject(String queueName) {
		final Subject<DequeuedMessageItem> result = pollingSubjectsRegistry.get(queueName);
		return result == null
				? computeAndCache(queueName, pollingSubjectsRegistry,
						s -> PublishSubject.<DequeuedMessageItem>create().toSerialized())
				: result;
	}

}
