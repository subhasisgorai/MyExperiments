package com.webworkz.playground.azure.queue.impl;

import static com.webworkz.playground.azure.common.ThrowingConsumer.unchecked;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Verify;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.models.MessageIDDeleteResponse;
import com.webworkz.playground.azure.queue.EventType;
import com.webworkz.playground.azure.queue.IAzureStorageQueueManager;
import com.webworkz.playground.azure.queue.Message;

public class AzureStorageQueueManagerTest {
	private static final String ENDPOINT_SUFFIX = "endpoint.suffix";
	private static final String ACCOUNT_KEY = "account.key";
	private static final String ACCOUNT_NAME = "account.name";
	private static final String DEFAULT_ENDPOINTS_PROTOCOL = "default.endpoints.protocol";
	private static final String TEST_CONFIG_PROPERTIES_FILE = "test-config.properties";
	private static final String MESSAGE_CONTENT = "Test Message [%d]";
	private static final String STORAGE_CONNECTION_STRING_PATTERN = "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;EndpointSuffix=%s";
	private static final int QUEUE_DELETION_COOLDOWN_PERIOD_SECONDS = 60;
	private static IAzureStorageQueueManager queueManager;

	@BeforeClass
	public static void setUp() throws InterruptedException, FileNotFoundException, IOException {
		Properties props = new Properties();
		props.load(ClassLoader.getSystemResourceAsStream(TEST_CONFIG_PROPERTIES_FILE));

		Verify.verify(props.size() > 0,
				"Please define the required properties before executing the tests in " + TEST_CONFIG_PROPERTIES_FILE);
		Verify.verify(StringUtils.isNotBlank(props.getProperty(DEFAULT_ENDPOINTS_PROTOCOL)),
				"Please configure the Azure Storage endpoints protocol in " + TEST_CONFIG_PROPERTIES_FILE);
		Verify.verify(StringUtils.isNotBlank(props.getProperty(ACCOUNT_NAME)),
				"Please configure the Azure Storage account name in " + TEST_CONFIG_PROPERTIES_FILE);
		Verify.verify(StringUtils.isNotBlank(props.getProperty(ACCOUNT_KEY)),
				"Please configure the Azure Storage account key in " + TEST_CONFIG_PROPERTIES_FILE);
		Verify.verify(StringUtils.isNotBlank(props.getProperty(ENDPOINT_SUFFIX)),
				"Please configure the Azure Storage endpoint suffix in " + TEST_CONFIG_PROPERTIES_FILE);

		System.out.println("Cleaning up existing queues ...");

		try {
			CloudStorageAccount storageAccount = CloudStorageAccount
					.parse(String.format(STORAGE_CONNECTION_STRING_PATTERN,
							props.getProperty(DEFAULT_ENDPOINTS_PROTOCOL), props.getProperty(ACCOUNT_NAME),
							props.getProperty(ACCOUNT_KEY), props.getProperty(ENDPOINT_SUFFIX)));
			CloudQueueClient queueClient = storageAccount.createCloudQueueClient();

			EnumSet.allOf(EventType.class)
					.forEach(unchecked(eventType -> deleteQueue(eventType.getQueueName(), queueClient)));

			System.out.println("Clean up initiated.\n");

			System.out.println("Cooling down, as queue deletion takes time");
			AzureStorageQueueManager.waitAndPrintDots(1, QUEUE_DELETION_COOLDOWN_PERIOD_SECONDS); // allowing some cool
																									// down period

		} catch (Throwable t) {
			System.out.println("Encountered error while deleting queues, cause: " + t.getMessage());
		}

		queueManager = new AzureStorageQueueManager.Builder().accountKey(props.getProperty(ACCOUNT_KEY))
				.accountName(props.getProperty(ACCOUNT_NAME)).build();

	}

	private static void deleteQueue(String queueName, CloudQueueClient queueClient)
			throws URISyntaxException, StorageException {
		CloudQueue queue = queueClient.getQueueReference(queueName);
		queue.delete();
	}

	/**
	 * Disclaimer: This is not a unit test in true sense rather integration test,
	 * external services are not mocked. Just starting with tests. Will make it
	 * better over the time.
	 */
	@Test
	public void messagesPostAndRetrieveCountMatch() {
		int numThreads = 5;
		CountDownLatch latch = new CountDownLatch(numThreads);
		List<String> expected = new ArrayList<>();
		List<String> actual = new ArrayList<>();

		IntStream.range(0, numThreads).mapToObj(i -> String.format(MESSAGE_CONTENT, (i + 1)))
				.forEach(messagePayload -> {
					expected.add(messagePayload);
					Message message = new Message(messagePayload, EventType.EVENT_A);
					queueManager.publish(message);
				});

		queueManager.startPollingQueue(EventType.EVENT_A.getQueueName(), dequeuedMessageItem -> {
			try {
				String messageText = dequeuedMessageItem.messageText();
				actual.add(messageText);
				System.out
						.println("Dequeued message: " + messageText + ", Thread: " + Thread.currentThread().getName());
				MessageIDDeleteResponse deleteMessageResponse = queueManager
						.deleteMessage(dequeuedMessageItem.messageId(), EventType.EVENT_A,
								dequeuedMessageItem.popReceipt())
						.blockingGet();
				System.out.println(String.format("Message Id [%s] delete operation response code was [%s]",
						dequeuedMessageItem.messageId(), deleteMessageResponse.statusCode()));

			} catch (Throwable t) {
				System.err.println("Error encountered while processing message" + t.getMessage());
			}
			latch.countDown();
		}, throwable -> {
			System.err.println("Encountered an error while dequeuing messages");
			Assert.fail("Exception " + throwable);
		});

		System.out.println("Thread: [" + Thread.currentThread().getName() + "] would be waiting");
		try {
			boolean ret = latch.await(5, TimeUnit.SECONDS);
			System.out.println("Timed out while waiting on latch? " + !ret);
		} catch (InterruptedException e) {
			System.out.println("Interrupted while waiting on latch");
			Assert.fail("Exception " + e);
		}

		queueManager.stopPolling();

		Collections.sort(expected);
		Collections.sort(actual);

		assertEquals(expected, actual);

	}

	@Test
	public void messagePostBlockingAndRetrieve() {
		String messageContent = "This is a sample message to be posted in blocking manner";
		CountDownLatch singleLatch = new CountDownLatch(1);
		queueManager.publishBlocking(new Message(messageContent, EventType.EVENT_B));
		queueManager.startPollingQueue(EventType.EVENT_B.getQueueName(), dequeuedMessageItem -> {
			try {
				String messageText = dequeuedMessageItem.messageText();
				System.out
						.println("Dequeued message: " + messageText + ", Thread: " + Thread.currentThread().getName());
				assertEquals(messageContent, messageText);
				MessageIDDeleteResponse deleteMessageResponse = queueManager
						.deleteMessage(dequeuedMessageItem.messageId(), EventType.EVENT_B,
								dequeuedMessageItem.popReceipt())
						.blockingGet();
				System.out.println(String.format("Message Id [%s] delete operation response code was [%s]",
						dequeuedMessageItem.messageId(), deleteMessageResponse.statusCode()));

			} catch (Throwable t) {
				System.err.println("Error encountered while processing message" + t.getMessage());
			}
			singleLatch.countDown();
		}, throwable -> {
			System.err.println("Encountered an error while dequeuing messages");
			singleLatch.countDown();
		});

		try {
			boolean ret = singleLatch.await(5, TimeUnit.SECONDS);
			System.out.println("Timed out while waiting on latch? " + !ret);
		} catch (InterruptedException e) {
			Assert.fail("Exception " + e);
		}
	}

	@AfterClass
	public static void cleanUp() throws Exception {
		if (queueManager != null)
			queueManager.close();
	}

}
