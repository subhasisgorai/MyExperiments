package com.webworkz.playground.pipeline;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ExportPipelineConfiguration {

	@JsonProperty
	private int pollingTimeoutSeconds;

	@JsonProperty
	private int threadPoolKeepAliveMilliSeconds;

	@JsonProperty
	private int threadPoolQueueingCapacity;

	@JsonProperty
	private int failedJobRetryCount;

	public int getPollingTimeoutSeconds() {
		return pollingTimeoutSeconds;
	}

	public int getThreadPoolKeepAliveMilliSeconds() {
		return threadPoolKeepAliveMilliSeconds;
	}

	public int getThreadPoolQueueingCapacity() {
		return threadPoolQueueingCapacity;
	}

	public int getFailedJobRetryCount() {
		return failedJobRetryCount;
	}

}