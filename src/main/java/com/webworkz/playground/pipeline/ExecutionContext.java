package com.webworkz.playground.pipeline;

public final class ExecutionContext {
	private final long executionId;

	public ExecutionContext(long executionId) {
		this.executionId = executionId;
	}

	public long getExecutionId() {
		return executionId;
	}

}
