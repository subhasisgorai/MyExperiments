package com.webworkz.playground.tester;

import java.util.Scanner;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Verify;
import com.webworkz.playground.azure.queue.EventType;
import com.webworkz.playground.azure.queue.IAzureStorageQueueManager;
import com.webworkz.playground.azure.queue.Message;
import com.webworkz.playground.azure.queue.impl.AzureStorageQueueManager;

public class QueueManagerTester {
	private static final Logger logger = LoggerFactory.getLogger(QueueManagerTester.class);

	public static void main(String... args) {
		String accountName = null;
		String accountKey = null;

		try (Scanner userInputScanner = new Scanner(System.in)) {
			System.out.print("Please provide the Azure Storage Account Name: ");
			accountName = userInputScanner.nextLine().trim();
			System.out.print("Please provide the Azure Storage Account Key: ");
			accountKey = userInputScanner.nextLine().trim();
		}

		Verify.verify(StringUtils.isNotBlank(accountName) && StringUtils.isNotBlank(accountKey),
				"Azure Storage Account Name/Key must not be null");

		logger.info("Using Azure Storage Account Name: " + accountName);
		logger.info("Using Storage Account Key: " + accountKey);

		try (IAzureStorageQueueManager queueManager = new AzureStorageQueueManager.Builder().accountKey(accountKey)
				.accountName(accountName).build()) {

			IntStream.range(0, 25).mapToObj(i -> new Message("This is message" + (i + 1), EventType.EVENT_A))
					.forEach(queueManager::publish);

			queueManager.startPollingQueue(EventType.EVENT_A.getQueueName(), dequeuedMessageItem -> {
				logger.info("Received message, id [{}], message content: [{}], " + "Dequeue count: [{}]",
						dequeuedMessageItem.messageId(), dequeuedMessageItem.messageText(),
						dequeuedMessageItem.dequeueCount());
				queueManager.deleteMessage(dequeuedMessageItem.messageId(), EventType.EVENT_A,
						dequeuedMessageItem.popReceipt());
			}, Throwable::printStackTrace);

			sleep(10_000);

		} catch (Exception e) {
			logger.error("Encountered error while working with AzureStorageQueueManager", e);
		}

	}

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
