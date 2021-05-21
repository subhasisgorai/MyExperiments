package com.webworkz.playground.pipeline;

import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline<T extends Event> implements Iterable<Stage<?>> {
	private ExecutionContext context;
	private final Stage<T> head;
	private Stage<? extends Event> tail;
	private int numStages;

	public static final String STAGE_NAME = "Stage_Name";
	public static final String EXECUTION_ID = "Execution_Id";
	public static final String EVENT_TYPE = "Event_Type";
	public static final String EVENT = "Event";

	private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

	public Pipeline(Stage<T> startingStage) {
		if (startingStage == null)
			throw new PipelineException("The starting stage shouldn't be null");
		this.head = startingStage;
		this.tail = startingStage;
		this.numStages = 1;
	}

	@Override
	public Iterator<Stage<?>> iterator() {
		return new Iterator<Stage<?>>() {
			private Stage<?> currentStage = head;

			@Override
			public boolean hasNext() {
				return currentStage != null;
			}

			@Override
			public Stage<?> next() {
				Stage<?> stage = currentStage;
				currentStage = currentStage.nextStage;
				return stage;
			}
		};
	}

	public void addStage(Stage<? extends Event> stage) {
		if (stage != null) {
			stage.previousStage = this.tail;
			this.tail.nextStage = stage;
			this.tail = stage;
			this.numStages++;
		}
	}

	public int getStagesCount() {
		return numStages;
	}

	public ExecutionContext getContext() {
		return context;
	}

	public void setExecutionContext(ExecutionContext context) {
		this.context = context;
		for (Stage<?> stage : this) {
			stage.setContext(context);
		}
	}

	public void startExecution(Collection<T> eventsForFirstStage) {
		assert context != null : "Set a valid execution context first and retry";

		head.startCascaded();
		eventsForFirstStage.stream().forEach(event -> {
			try {
				head.addEventForProcessing(event);
			} catch (InterruptedException e) {
				logger.error("Interrupted while trying to add an event. Details: {} [{}], {} [{}], {} [{}], {}",
						EXECUTION_ID, context.getExecutionId(), STAGE_NAME, head.name, EVENT_TYPE, event.getType(),
						EVENT, event);
			}
		});
	}

	public void stopExecution() {
		head.requestStopCascaded();
	}

	public void killExecution() {
		for (Stage<?> stage : this) {
			stage.stopPrematurely();
		}
	}

}
