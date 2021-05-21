package com.webworkz.playground.azure.queue.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Verify;
import com.microsoft.azure.storage.queue.MessageIdURL;
import com.microsoft.azure.storage.queue.MessagesURL;
import com.microsoft.azure.storage.queue.QueueURL;
import com.microsoft.azure.storage.queue.ServiceURL;
import com.microsoft.azure.storage.queue.SharedKeyCredentials;
import com.microsoft.azure.storage.queue.StorageURL;
import com.microsoft.azure.storage.queue.models.DequeuedMessageItem;
import com.microsoft.azure.storage.queue.models.MessageEnqueueResponse;
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse;
import com.microsoft.azure.storage.queue.models.QueueCreateResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import com.webworkz.playground.azure.queue.EventType;
import com.webworkz.playground.azure.queue.IAzureStorageQueueManager;
import com.webworkz.playground.azure.queue.LazyCache;
import com.webworkz.playground.azure.queue.Message;
import com.webworkz.playground.azure.queue.ProcessingTypeState;
import com.webworkz.playground.common.IAuditable;

import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

/**
 * A concrete implementation of {@link IAzureStorageQueueManager}. This
 * implementation mostly abides by the reactive paradigm and tried to leverage
 * multithreading wherever possible.
 *
 */
public final class AzureStorageQueueManager implements IAzureStorageQueueManager, IAuditable {
	private final String accountName;
	private final String accountKey;
	private final LazyCache lazyCache;
	private final String[] queueNames;
	private final Phaser messageEnqueuePhaser;
	private final Subject<Message> messageEnqueuingSubject;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = readWriteLock.readLock();
	private final Lock writeLock = readWriteLock.writeLock();

	@GuardedBy("readWriteLock")
	private EnumMap<ProcessingTypeState, Long> bookkeeper = new EnumMap<>(ProcessingTypeState.class);

	private volatile boolean stopPolling = false;

	private static final int MAX_MESSAGE_PUBLISHING_CONCURRENCY = 6;
	private static final int MAX_MESSAGE_PROCESSING_CONCURRENCY = 10;
	private static final int MESSAGE_DEQUEUING_BATCH_SIZE = 20;
	private static final int VISIBILITY_TIMEOUT_SECONDS = 60;
	private static final int INVALID_NUM = -99999;
	private static final String STORAGE_QUEUE_URL_PATTERN = "https://%s.queue.core.windows.net";

	private static final Logger logger = LoggerFactory.getLogger(AzureStorageQueueManager.class);

	private AzureStorageQueueManager(Builder configuration) throws InterruptedException {
		Verify.verify(StringUtils.isNotBlank(configuration.accountName), "Account Name is REQUIRED!");
		Verify.verify(StringUtils.isNotBlank(configuration.accountKey), "Account Key is REQUIRED!");

		accountName = configuration.accountName;
		accountKey = configuration.accountKey;
		messageEnqueuingSubject = PublishSubject.<Message>create().toSerialized();

		messageEnqueuePhaser = new Phaser(1);

		SharedKeyCredentials credential = new SharedKeyCredentials(accountName, accountKey);
		HttpPipeline pipeline = StorageURL.createPipeline(credential);

		URL url = null;
		try {
			url = new URL(String.format(STORAGE_QUEUE_URL_PATTERN, accountName));
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("MalformedURLException was thrown, may be no/unknown protocol passed");
		}

		Verify.verify(url != null, "URL can't be null!");
		Verify.verify(pipeline != null, "HttpPipeline can't be null");

		lazyCache = new LazyCache(new ServiceURL(url, pipeline));

		queueNames = EnumSet.allOf(EventType.class).stream().map(e -> e.getQueueName()).toArray(String[]::new);

		CountDownLatch queueSetUpLatch = new CountDownLatch(1);
		createQueuesBlocking(queueNames, queueSetUpLatch);
		queueSetUpLatch.await();

		initiliazeBookkeeping();

		startRoutingAndEnqueueingMessages();

		logger.info("Azure Storage Queue Manager instantiated correctly\n");

	}

	private void initiliazeBookkeeping() {
		bookkeeper.put(ProcessingTypeState.INBOUND_PROCESSED, new Long(0));
		bookkeeper.put(ProcessingTypeState.INBOUND_FAILED, new Long(0));
		bookkeeper.put(ProcessingTypeState.OUTBOUND_SUCCESS, new Long(0));
		bookkeeper.put(ProcessingTypeState.OUTBOUND_FAILED, new Long(0));
	}

	private void incrementCount(ProcessingTypeState processingTypeStatus) {
		writeLock.lock();
		try {
			bookkeeper.merge(processingTypeStatus, 1L, Long::sum);
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public long getStats(ProcessingTypeState processingTypeState) {
		long count = INVALID_NUM;
		readLock.lock();
		try {
			if (bookkeeper.get(processingTypeState) != null)
				count = bookkeeper.get(processingTypeState);
		} finally {
			readLock.unlock();
		}
		return count;
	}

	@Override
	public Map<ProcessingTypeState, Long> getStatsSnapshot() {
		readLock.lock();
		try {
			return Collections.unmodifiableMap(bookkeeper);
		} finally {
			readLock.unlock();
		}
	}

	public void printStats() {
		logger.info("\n{}\n", Arrays.toString(bookkeeper.entrySet().toArray()));
	}

	public static class Builder {
		private String accountName;
		private String accountKey;

		public Builder accountName(String value) {
			accountName = value;
			return this;
		}

		public Builder accountKey(String value) {
			accountKey = value;
			return this;
		}

		public AzureStorageQueueManager build() throws InterruptedException {
			return new AzureStorageQueueManager(this);
		}
	}

	private void createQueuesBlocking(String[] queueNames, CountDownLatch latch) {
		Observable.fromArray(queueNames).flatMapCompletable(queueName -> Completable.fromRunnable(() -> {
			logger.info("Creating Queue [{}]", queueName);
			createQueueBlocking(queueName);
		}).subscribeOn(Schedulers.io())).subscribe(() -> {
			logger.info("done setting up the queues");
			latch.countDown();
		}, throwable -> {
			logger.error("Encountered error while creating queues", throwable);
			System.exit(-1);
		});

	}

	private void startRoutingAndEnqueueingMessages() {
		messageEnqueuingSubject.flatMap(m -> Observable.just(m).subscribeOn(Schedulers.io()).map(message -> {
			try {
				String queueName = message.getEventType().getQueueName();
				MessagesURL messagesURL = lazyCache.getOrCreateMessagesURL(queueName);
				logger.debug("Enqueuing message: [{}]", message.getMessageText());
				return messagesURL.enqueue(message.getMessageText());
			} catch (Throwable t) {
				logger.error("Error encountered while posting message [{}]", t);
				return null;
			}
		}), MAX_MESSAGE_PUBLISHING_CONCURRENCY).subscribeOn(Schedulers.io()).subscribe(singleResponse -> {
			singleResponse.subscribe(messageEnqueueResponse -> {
				if (messageEnqueueResponse != null) {
					logger.debug("Message posting request/response status code: {}",
							messageEnqueueResponse.statusCode());
					logger.debug("Successfully enqueued the message");
					incrementCount(ProcessingTypeState.INBOUND_PROCESSED);
				} else {
					logger.error("Retrieved a null response, probably posting operation failed earlier");
					incrementCount(ProcessingTypeState.INBOUND_FAILED);
				}
				messageEnqueuePhaser.arriveAndDeregister();
			}, throwable -> {
				logger.error("Encountered Error while posting message", throwable);
			});
		});

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void startPollingQueue(String queueName, Consumer<DequeuedMessageItem> onNext, Consumer<Throwable> onError) {
		Subject<DequeuedMessageItem> subject = lazyCache.getOrCreatePollingSubject(queueName);
		subject.subscribe(onNext, onError);

		Observable.interval(1, TimeUnit.SECONDS).takeWhile(l -> !stopPolling)
				.flatMap(l -> Observable.just(l).subscribeOn(Schedulers.io()).map(tick -> {
					try {
						MessagesURL messagesURL = lazyCache.getOrCreateMessagesURL(queueName);
						logger.debug("Dequeuing for Queue: [{}]", queueName);
						return messagesURL.dequeue(MESSAGE_DEQUEUING_BATCH_SIZE, VISIBILITY_TIMEOUT_SECONDS);
					} catch (Throwable t) {
						logger.error("Error encountered while dequeuing message", t);
						return null;
					}
				}), MAX_MESSAGE_PROCESSING_CONCURRENCY).subscribeOn(Schedulers.io()).subscribe(singleResponse -> {
					singleResponse.subscribe(messageDequeueResponse -> {
						if (messageDequeueResponse != null) {
							List<DequeuedMessageItem> messagesDequeued = messageDequeueResponse.body();
							if (messagesDequeued != null)
								messagesDequeued.forEach(message -> {
									incrementCount(ProcessingTypeState.OUTBOUND_SUCCESS);
									subject.onNext(message);
								});
							else
								incrementCount(ProcessingTypeState.OUTBOUND_FAILED);
						}
					}, throwable -> logger.error("Encountered Error while posting message", throwable));
				});
	}

	/**
	 * {@inheritDoc} <br>
	 * {@link AzureStorageQueueManager#MAX_MESSAGE_PUBLISHING_CONCURRENCY} controls
	 * the degree of parallelization of message publish.
	 */
	@Override
	public void publish(Message message) {
		Verify.verify(message != null, "Message being passed to be published shouldn't be null");

		messageEnqueuePhaser.register();
		messageEnqueuingSubject.onNext(message);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Single<MessageIDDeleteResponse> deleteMessage(String messageId, EventType eventType, String popReceipt) {
		MessagesURL messagesURL = lazyCache.getOrCreateMessagesURL(eventType.getQueueName());
		MessageIdURL messageIdURL = messagesURL.createMessageIdUrl(messageId);
		return messageIdURL.delete(popReceipt);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stopPolling() {
		stopPolling = true;
	}

	@Override
	public void close() {
		logger.info("\nClosing AzureStorageQueueManager ...");
		messageEnqueuePhaser.arriveAndAwaitAdvance();
		printStats();
		logger.info("AzureStorageQueueManager closed");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createQueueBlocking(String queueName) {
		Verify.verify(StringUtils.isNotBlank(queueName),
				"Queue name passed while attempting to create a queue shouldn't be null or blank");

		QueueURL qu = lazyCache.getOrCreateQueueURL(queueName);
		QueueCreateResponse queueCreateResponse = qu.create().blockingGet();

		logger.debug("Queue [{}] creation operation response code was [{}]", queueName,
				queueCreateResponse.statusCode());
	}

	@Override
	public void publishBlocking(Message message) {
		Verify.verify(message != null, "Message being passed to be published shouldn't be null");

		String queueName = message.getEventType().getQueueName();
		MessagesURL messagesURL = lazyCache.getOrCreateMessagesURL(queueName);
		String messageText = message.getMessageText();
		MessageEnqueueResponse messageEnqueueResponse = messagesURL.enqueue(messageText).blockingGet();
		String responseCode = messageEnqueueResponse != null ? String.valueOf(messageEnqueueResponse.statusCode())
				: null;
		logger.debug("Message [{}] enqueue operation response code was [{}]", messageText, responseCode);
		if (StringUtils.isNotBlank(responseCode) && responseCode.startsWith("2"))
			incrementCount(ProcessingTypeState.INBOUND_PROCESSED);
		else
			incrementCount(ProcessingTypeState.INBOUND_FAILED);

	}

	public static void waitAndPrintDots(long initialDelaySeconds, long waitDurationSeconds) {
		ScheduledExecutorService dotter = Executors.newScheduledThreadPool(1);
		dotter.scheduleAtFixedRate(() -> System.out.print("."), initialDelaySeconds, 1, TimeUnit.SECONDS);
		try {
			Thread.sleep(waitDurationSeconds * 1_000);
		} catch (InterruptedException e) {
			logger.error("Interrupted while waiting", e);
		}
		System.out.println("\n");
		dotter.shutdownNow();
	}

}