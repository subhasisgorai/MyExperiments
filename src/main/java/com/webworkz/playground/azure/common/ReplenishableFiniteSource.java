package com.webworkz.playground.azure.common;

import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.microsoft.azure.storage.ResultSegment;

/**
 * A {@link Supplier} implementation that prefetches data from underlying
 * storage and buffers them temporarily for consumption.<b> As soon as the
 * buffer's content falls below a certain threshold it makes an attempt to
 * replenish it by fetching more data from the data storage as long as there is
 * further data available.
 *
 * @param <T> parameterized type of the data in the underlying storage
 */
public class ReplenishableFiniteSource<T> implements Supplier<T> {
	private final float thresholdToFetchMoreItemsFromSource;
	private final Supplier<ResultSegment<T>> dataSupplier;

	private volatile List<T> primaryBuffer;
	private volatile List<T> reserveBuffer;
	private volatile int pagesRead;
	private volatile int itemsRead;
	private volatile ResultSegment<T> currentResultSegment;

	private AtomicBoolean dataFetchingStarted = new AtomicBoolean(false);

	public ReplenishableFiniteSource(Supplier<ResultSegment<T>> dataSupplier, float thresholdPercentage) {
		thresholdToFetchMoreItemsFromSource = thresholdPercentage;
		this.dataSupplier = dataSupplier;
	}

	/**
	 * {@inheritDoc} <br>
	 * As mentioned earlier, data is prefetched from underlying store to the
	 * intermediate buffer in order to avoid delay introduced for fetching data from
	 * the data storage (more often over network). <br>
	 * Clients would mostly get data from the buffer, hence retrieval would be quite
	 * performant.
	 */
	@Override
	public T get() {
		if (isEmpty(primaryBuffer) || itemsRead >= primaryBuffer.size()) {
			if (shouldFetchDataFromSupplier() && dataFetchingStarted.compareAndSet(false, true))
				populateSecondaryBufferFromDataSupplier();
			waitUntilDataCompletelyLoaded();
			flipBuffer();
		}
		return (isNotEmpty(primaryBuffer) && itemsRead < primaryBuffer.size()) ? readItemAndPrefetchIfNeeded() : null;
	}

	private T readItemAndPrefetchIfNeeded() {
		T item = primaryBuffer.get(itemsRead++);
		if (((float) itemsRead / primaryBuffer.size()) > thresholdToFetchMoreItemsFromSource) {
			if (shouldFetchDataFromSupplier() && dataFetchingStarted.compareAndSet(false, true))
				CompletableFuture.runAsync(this::populateSecondaryBufferFromDataSupplier)
						.whenComplete((result, error) -> {
							if (error != null) {
								throw new RuntimeException(error);
							}
						});
		}
		return item;
	}

	private synchronized void flipBuffer() {
		primaryBuffer = reserveBuffer;
		reserveBuffer = null;
		itemsRead = 0;
		pagesRead++;
	}

	private void waitUntilDataCompletelyLoaded() {
		synchronized (this) {
			while (dataFetchingStarted.get())
				try {
					wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}

		}
	}

	private synchronized void populateSecondaryBufferFromDataSupplier() {
		currentResultSegment = dataSupplier.get();
		reserveBuffer = currentResultSegment.getResults();
		dataFetchingStarted.compareAndSet(true, false);
		notifyAll();
	}

	private synchronized boolean shouldFetchDataFromSupplier() {
		return reserveBuffer == null && (currentResultSegment == null
				|| (currentResultSegment != null && currentResultSegment.getHasMoreResults()));
	}

}
