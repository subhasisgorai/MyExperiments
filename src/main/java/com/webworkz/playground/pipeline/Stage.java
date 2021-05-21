package com.webworkz.playground.pipeline;

import static com.webworkz.playground.pipeline.Pipeline.EVENT_TYPE;
import static com.webworkz.playground.pipeline.Pipeline.EXECUTION_ID;
import static com.webworkz.playground.pipeline.Pipeline.STAGE_NAME;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.webworkz.playground.common.IAuditable;

/**
 * 
 * Stage is the <b>crux of the Data processing Pipeline</b>: the structural &
 * functional unit.
 * <p>
 * Blocking queue coupled with a Thread Pool supports the Producer-Consumer
 * design pattern. Producer-Consumer pattern removes the code dependencies
 * between producer and consumer classes, and simplifies workload management by
 * decoupling activities that may produce or consume data at different or
 * variable rates. The {@link BlockingQueue} implementations contains sufficient
 * internal synchronization to safely publish objects from a producer thread to
 * the consumer thread, also helps in a clean transfer of ownership.
 * <p>
 * Internally {@link Stage} maintains a Storage ({@link BlockingQueue}) and a
 * pool of threads {@link FaultTolerantThreadPool}. Stages collectively
 * constitutes a Pipeline per Streaming Event Type. Upstream Stages add Events
 * for processing to the internal queue, individual thread from pool picks
 * queued events up, processes and may push them to the downstream Stage for
 * subsequent processing.
 * <p>
 * Default BlockinQueue is {@link LinkedBlockingQueue} which is not bounded by
 * nature, so precaution must be taken to ensure that it does not hog JVM memory
 * and subsequently leads to an {@link OutOfMemoryError}.
 * <p>
 * Additionally, it maintains Processing Statistics for an Execution Audit and
 * Metrics.
 * 
 * @see Pipeline
 *
 * @param <T> The type of event this Stage will hold and process
 */
@ThreadSafe
public abstract class Stage<T extends Event> implements Startable, Stoppable, IAuditable {
	private ExecutionContext context;

	protected final int numThreads;
	protected final String name;
	protected final BlockingQueue<T> internalStorage;
	protected final FaultTolerantThreadPool executor;
	protected final ExportPipelineConfiguration config;

	protected StageState state = StageState.UNKNOWN;

	protected volatile boolean stopRequested = false;

	protected Stage<? extends Event> nextStage;
	protected Stage<? extends Event> previousStage;

	protected static final int DEFAULT_STEP_SIZE = 1;
	protected static final int INVALID_NUM = -99999;
	protected static final int NULL_STR = -99999;

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	@GuardedBy("readWriteLock")
	protected EnumMap<ProcessingState, Long> statusStats = new EnumMap<>(ProcessingState.class);

	private static final Logger logger = LoggerFactory.getLogger(Stage.class);

	public Stage(String name, int numThreads, ExportPipelineConfiguration config, BlockingQueue<T> internalStorage) {
		this.name = name;
		this.internalStorage = internalStorage;
		this.numThreads = numThreads;
		this.config = config;
		this.executor = new FaultTolerantThreadPool(numThreads, numThreads);

		initiliazeMetrics();

	}

	public Stage(String name, int numThreads, ExportPipelineConfiguration config) {
		this(name, numThreads, config, new LinkedBlockingQueue<T>());
	}

	@Override
	public void start() {
		assert context != null
				: "Can't initialize the stage without an Execution Context, please set the execution context first and retry";

		executor.prestartAllCoreThreads();

		for (int i = 0; i < numThreads; i++) {
			executor.submit(new Worker());
		}
		state = StageState.ACTIVE;
		executor.shutdown();
	}

	@Override
	public void startCascaded() {
		start();
		if (nextStage != null)
			nextStage.startCascaded();

	}

	@Override
	public boolean isStarted() {
		return state != StageState.UNKNOWN;
	}

	private void initiliazeMetrics() {
		statusStats.put(ProcessingState.QUEUED, new Long(0));
		statusStats.put(ProcessingState.IN_PROCESS, new Long(0));
		statusStats.put(ProcessingState.PROCESSED, new Long(0));
		statusStats.put(ProcessingState.FAILED, new Long(0));
		statusStats.put(ProcessingState.ADDED, new Long(0));
	}

	protected abstract void processEvent(T t) throws Exception;

	protected void finalizeEventProcessing(T t) {
		// NOOP Sub-classes are free to override this method.
	}

	protected abstract void handleFailedEvent(T t, Throwable error);

	protected void handleFailedJob(Runnable r) {
		state = StageState.FAILED;
	}

	protected void incrementStatusCount(ProcessingState processingStatus, long step) {
		write.lock();
		try {
			statusStats.merge(processingStatus, step, Long::sum);
		} finally {
			write.unlock();
		}
	}

	protected void decrementStatusCount(ProcessingState processingStatus, long step) {
		write.lock();
		try {
			statusStats.merge(processingStatus, step, (prevValue, stepValue) -> prevValue - stepValue);
		} finally {
			write.unlock();
		}
	}

	protected abstract void shutdownHook();

	protected class Worker implements Runnable {
		@Override
		public void run() {
			try {
				long timeout = config.getPollingTimeoutSeconds();

				modifyCurrentThreadName("%s-%s", Thread.currentThread().getName(), name);

				while (true && !Thread.currentThread().isInterrupted()) {
					if (stopRequested && internalStorage.isEmpty() && isPreviousStageOver()) {
						executor.decermentActiveThreadCount();
						if (executor.getActiveThreadCount() < 1) {
							logger.info("Stage is stopped {}={}, {}={}", EXECUTION_ID, getExecutionId(), STAGE_NAME,
									name);
							if (state != StageState.FAILED)
								state = StageState.STOPPED;
							try {
								shutdownHook();
							} catch (Throwable error) {
								// alien implementation of shutdown hook, needs to be
								// guarded against exceptional situations
								logger.error("Execution Shutdown Hook failed. Details: {} [{}], {} [{}] ", EXECUTION_ID,
										getExecutionId(), STAGE_NAME, name, error);
							}
						}
						return;
					}

					T t = internalStorage.poll(timeout, TimeUnit.SECONDS);

					if (t == null) {
						logger.error("Encountered a null event and ignored, Details: {} [{}], {} [{}]", EXECUTION_ID,
								getExecutionId(), STAGE_NAME, name);
						continue;
					}

					int maxRetries = config.getFailedJobRetryCount();

					incrementStatusCount(ProcessingState.IN_PROCESS, DEFAULT_STEP_SIZE);
					decrementStatusCount(ProcessingState.QUEUED, DEFAULT_STEP_SIZE);
					try {
						processWithRetry(t, maxRetries);

					} catch (Throwable throwable) {
						logger.error("Event was not processed successfully. Details: {} [{}], {} [{}], {} [{}]",
								EXECUTION_ID, getExecutionId(), STAGE_NAME, name, EVENT_TYPE, t.getType(), throwable);

						try {
							handleFailedEvent(t, throwable);
						} catch (Throwable error) {
							// around an alien implementation, needs special
							// handling
							logger.error("handleFailedRecord failed. Details: {} [{}], {} [{}], {} [{}]", EXECUTION_ID,
									getExecutionId(), STAGE_NAME, name, EVENT_TYPE, t.getType(), error);
						}
					} finally {
						finalizeEventProcessing(t);
					}
				}
			} catch (InterruptedException e) {
				logger.info("Interrupted, so coming out");
				Thread.currentThread().interrupt();
			}
		}

		private void processWithRetry(T t, int maxAttempts) throws Exception {
			for (int retries = 0;; retries++) {
				try {
					processEvent(t);
					incrementStatusCount(ProcessingState.PROCESSED, DEFAULT_STEP_SIZE);
					decrementStatusCount(ProcessingState.IN_PROCESS, DEFAULT_STEP_SIZE);
				} catch (Throwable e) {
					if (retries < maxAttempts) {
						continue;
					} else {
						throw e;
					}
				}
			}

		}

	}

	protected boolean isPreviousStageOver() {
		return previousStage == null || isStageOver(previousStage);
	}

	public static boolean isStageOver(Stage<?> stage) {
		return stage != null && (stage.state == StageState.STOPPED || stage.state == StageState.FAILED);
	}

	public StageState getState() {
		return state;
	}

	public String getName() {
		return name;
	}

	public ExecutionContext getContext() {
		return context;
	}

	public long getExecutionId() {
		return getContext() != null ? getContext().getExecutionId() : NULL_STR;
	}

	public void setContext(ExecutionContext context) {
		this.context = context;
	}

	public long getStats(ProcessingState status) {
		long count = INVALID_NUM;
		read.lock();
		try {
			if (statusStats.get(status) != null)
				count = statusStats.get(status);
		} finally {
			read.unlock();
		}
		return count;
	}

	public Map<ProcessingState, Long> getStats() {
		read.lock();
		try {
			return Collections.unmodifiableMap(statusStats);
		} finally {
			read.unlock();
		}
	}

	public void addEventForProcessing(T t) throws InterruptedException {
		if (!isStarted())
			throw new PipelineException("State is not initialized yet, " + EXECUTION_ID + "[" + getExecutionId() + "],"
					+ STAGE_NAME + "[" + name + "]. Please initialize before adding events for processing");
		if (stopRequested && isPreviousStageOver())
			throw new PipelineException(
					"Already stop-request had happened, no more event addition for processing is allowed, "
							+ EXECUTION_ID + "[" + getExecutionId() + "]," + STAGE_NAME + "[" + name + "]");
		incrementStatusCount(ProcessingState.ADDED, DEFAULT_STEP_SIZE);
		addEventForProcessingInternally(t);
	}

	private void addEventForProcessingInternally(T t) throws InterruptedException {
		try {
			internalStorage.put(t);
			incrementStatusCount(ProcessingState.QUEUED, DEFAULT_STEP_SIZE);
		} catch (InterruptedException e) {
			logger.info("Interrupted while adding event for processing");
			throw e;
		}
	}

	public static void modifyCurrentThreadName(String pattern, Object... args) {
		String newThreadName = String.format(pattern, args);
		Thread.currentThread().setName(newThreadName);

	}

	@Override
	public boolean isStopRequested() {
		return stopRequested;
	}

	@Override
	public void requestStop() {
		stopRequested = true;
		state = StageState.STOP_REQUESTED;
	}

	@Override
	public void requestStopCascaded() {
		requestStop();
		if (nextStage != null)
			nextStage.requestStopCascaded();

	}

	@Override
	public void stopPrematurely() {
		logger.warn("Received a request for premature stop, {} [{}], {} [{}]", EXECUTION_ID, getExecutionId(),
				STAGE_NAME, name);
		if (executor != null)
			executor.shutdownNow();
	}

	protected class FaultTolerantThreadPool extends ThreadPoolExecutor {

		private int noOfActiveThreads = 0;

		public FaultTolerantThreadPool(int corePoolSize, int maximumPoolSize) {
			super(corePoolSize, maximumPoolSize, config.getThreadPoolKeepAliveMilliSeconds(), TimeUnit.MILLISECONDS,
					new ArrayBlockingQueue<>(config.getThreadPoolQueueingCapacity()),
					new ThreadFactoryBuilder().setNameFormat("StageWorkerPool-%d").build());
		}

		public synchronized int getActiveThreadCount() {
			return noOfActiveThreads;
		}

		public synchronized void incrementActiveThreadCount() {
			noOfActiveThreads++;
		}

		public synchronized void decermentActiveThreadCount() {
			if (noOfActiveThreads > 0) {
				noOfActiveThreads--;
			}
		}

		@Override
		protected void beforeExecute(Thread t, Runnable r) {
			super.beforeExecute(t, r);
			incrementActiveThreadCount();
		}

		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			super.afterExecute(r, t);
			if (t == null && r instanceof Future<?>) {
				try {
					Future<?> future = (Future<?>) r;
					if (future.isDone())
						future.get();
				} catch (InterruptedException | ExecutionException e) {
					t = e.getCause();
				}
			}
			if (t != null) {
				logger.error("Worker encountered an error", t);
				if (r instanceof Stage.Worker)
					handleFailedJob(r);
			}
		}

	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31).append(getExecutionId()).append(getName()).toHashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof Stage))
			return false;
		final Stage<?> otherObject = (Stage<?>) obj;
		return new EqualsBuilder().append(getExecutionId(), otherObject.getExecutionId()).append(name, otherObject.name)
				.isEquals();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append(EXECUTION_ID, getExecutionId()).append(STAGE_NAME, name).toString();
	}

}
