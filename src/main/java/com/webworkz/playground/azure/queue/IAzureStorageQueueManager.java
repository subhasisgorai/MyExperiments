package com.webworkz.playground.azure.queue;

import com.microsoft.azure.storage.queue.models.DequeuedMessageItem;
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse;

import io.reactivex.Single;
import io.reactivex.functions.Consumer;

/**
 * Reactive interface that defines various methods and operations on Azure
 * Storage Queue
 *
 */
public interface IAzureStorageQueueManager extends AutoCloseable {
	/**
	 * Method to create a queue in Azure Storage Service. This is a blocking method
	 * as the name suggests.
	 * 
	 * @param queueName name of the queue to be created
	 */
	void createQueueBlocking(String queueName);

	/**
	 * {@link Message} instance is enqueued for being published.
	 * 
	 * @param message the message instance to be published
	 */
	void publish(Message message);

	/**
	 * Same as the other publish counterpart
	 * {@link IAzureStorageQueueManager#publish(Message)}, only difference being
	 * this method blocks till the publish attempt is completed.
	 * 
	 * @param message the message instance to be published
	 */
	void publishBlocking(Message message);

	/**
	 * This method is to initiate the polling on a particular queue for Azure
	 * Storage Service.
	 * 
	 * @param queueName the name of the queue to be polled
	 * @param onNext    consumer that processes a dequeued message from the queue
	 * @param onError   consumer that handles any error notification from the poller
	 */
	void startPollingQueue(String queueName, Consumer<DequeuedMessageItem> onNext, Consumer<Throwable> onError);

	/**
	 * This method instructs all the poller threads to stop
	 */
	void stopPolling();

	/**
	 * This method responsible for deleting a message from Azure Storage Queue. This
	 * returns a {@link Single} that could be subscribed for successful deletion or
	 * a failure outcome
	 * 
	 * @param messageId  the message with the message id to be deleted
	 * @param eventType  {@link EventType} of the message being deleted, based on
	 *                   this the queue would be chosen
	 * @param popReceipt A valid pop receipt value returned from an earlier call to
	 *                   the Get Messages or Update Message operation
	 * @return {@link Single} for successful deletion or a failure outcome
	 */
	Single<MessageIDDeleteResponse> deleteMessage(String messageId, EventType eventType, String popReceipt);

}
